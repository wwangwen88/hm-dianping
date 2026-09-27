package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.RedisIdWorker;
import com.hmdp.utils.UserHolder;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.context.ApplicationContext;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.connection.stream.Consumer;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.ReadOffset;
import org.springframework.data.redis.connection.stream.StreamOffset;
import org.springframework.data.redis.connection.stream.StreamReadOptions;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.annotation.Resource;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Slf4j
@Service
public class VoucherOrderServiceImpl extends ServiceImpl<VoucherOrderMapper, VoucherOrder> implements IVoucherOrderService {
    @Resource
    private ISeckillVoucherService seckillVoucherService;
    @Resource
    private RedisIdWorker redisIdWorker;
    @Resource
    private StringRedisTemplate stringRedisTemplate;
    @Resource
    private RedissonClient redissonClient;
    @Resource
    private ApplicationContext applicationContext;

    /**
     * 秒杀 Lua 脚本
     * 把「判断库存 + 判断一人一单 + 扣减库存 + 发消息到 Stream」合并成一次原子操作，
     * 由 Redis 单线程执行，天然避免超卖和重复下单。
     */
    private static final DefaultRedisScript<Long> SECKILL_SCRIPT;

    static {
        SECKILL_SCRIPT = new DefaultRedisScript<>();
        SECKILL_SCRIPT.setLocation(new ClassPathResource("seckill.lua"));
        SECKILL_SCRIPT.setResultType(Long.class);
    }

    /**
     * Redis Stream 相关常量
     * 用 Stream 取代 JVM 内存里的阻塞队列：消息持久化在 Redis，
     * 服务重启/宕机不会丢单，并且支持 ACK 确认与 pending 重投。
     */
    private static final String QUEUE_NAME = "stream.orders";
    private static final String GROUP_NAME = "g1";
    private static final String CONSUMER_NAME = "c1";

    /**
     * 异步下单的执行器：单线程，保证订单串行落库。
     * 用「守护线程」，否则它会阻止 JVM 退出（会导致测试跑不完）。
     */
    private static final ExecutorService SECKILL_ORDER_EXECUTOR =
            Executors.newSingleThreadExecutor(r -> {
                Thread t = new Thread(r, "seckill-order-handler");
                t.setDaemon(true);
                return t;
            });

    @PostConstruct
    private void init() {
        // 1.创建消费组（已经存在会抛异常，忽略即可）
        try {
            stringRedisTemplate.opsForStream().createGroup(QUEUE_NAME, ReadOffset.from("0"), GROUP_NAME);
            log.info("创建 Stream 消费组成功：{} / {}", QUEUE_NAME, GROUP_NAME);
        } catch (Exception e) {
            log.info("消费组已存在，无需重复创建：{} / {}", QUEUE_NAME, GROUP_NAME);
        }
        // 2.启动后台消费线程
        SECKILL_ORDER_EXECUTOR.submit(new VoucherOrderHandler());
    }

    @PreDestroy
    private void destroy() {
        SECKILL_ORDER_EXECUTOR.shutdownNow();
    }

    /**
     * 后台消费任务：不断从 Stream 读取订单消息并异步落库
     */
    private class VoucherOrderHandler implements Runnable {
        @Override
        public void run() {
            // 启动时先把「上次遗留的 pending 消息」处理掉。
            // 上次若是崩溃退出（没来得及 ACK），消息会留在 pending 列表里，
            // 教程只在 catch 里处理 pending，那种情况下这些消息会一直卡住，这里补上。
            handlePendingList();
            while (true) {
                try {
                    // 1.获取消息队列中的订单信息
                    //   XREADGROUP GROUP g1 c1 COUNT 1 BLOCK 2000 STREAMS stream.orders >
                    //   ">" 表示读取「从未被消费过」的新消息
                    List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(
                            Consumer.from(GROUP_NAME, CONSUMER_NAME),
                            StreamReadOptions.empty().count(1).block(Duration.ofSeconds(2)),
                            StreamOffset.create(QUEUE_NAME, ReadOffset.lastConsumed())
                    );
                    // 2.判断消息获取是否成功
                    if (list == null || list.isEmpty()) {
                        // 2.1.没有消息，继续下一次循环
                        continue;
                    }
                    // 3.解析消息中的订单信息
                    MapRecord<String, Object, Object> record = list.get(0);
                    Map<Object, Object> values = record.getValue();
                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(values, new VoucherOrder(), true);
                    // 4.获取成功，异步下单
                    handleVoucherOrder(voucherOrder);
                    // 5.ACK 确认，告诉 Redis 这条消息已经处理完了
                    stringRedisTemplate.opsForStream().acknowledge(QUEUE_NAME, GROUP_NAME, record.getId());
                } catch (Exception e) {

                    log.error("处理订单异常", e);
                    // 出现异常，去处理 pending-list（已投递但未确认的消息）
                    handlePendingList();
                }
            }
        }

        /**
         * 处理 pending-list：
         * 消费者读取了消息但还没来得及 ACK 就崩了，消息会留在 pending 列表里，
         * 这里把它们捞出来重新处理，避免丢单。
         */
        private void handlePendingList() {
            while (true) {
                try {
                    // 1.获取 pending-list 中的订单信息
                    //   XREADGROUP GROUP g1 c1 COUNT 1 STREAMS stream.orders 0
                    //   "0" 表示从 pending 列表的「开头」开始读
                    List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(
                            Consumer.from(GROUP_NAME, CONSUMER_NAME),
                            StreamReadOptions.empty().count(1),
                            StreamOffset.create(QUEUE_NAME, ReadOffset.from("0"))
                    );
                    // 2.判断消息获取是否成功
                    if (list == null || list.isEmpty()) {
                        // 2.1.没有异常消息，结束循环
                        break;
                    }
                    // 3.解析消息中的订单信息
                    MapRecord<String, Object, Object> record = list.get(0);
                    Map<Object, Object> values = record.getValue();
                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(values, new VoucherOrder(), true);
                    // 4.获取成功，异步下单
                    handleVoucherOrder(voucherOrder);
                    // 5.ACK 确认
                    stringRedisTemplate.opsForStream().acknowledge(QUEUE_NAME, GROUP_NAME, record.getId());
                } catch (Exception e) {
                    log.error("处理 pending-list 异常", e);
                    try {
                        Thread.sleep(20);
                    } catch (InterruptedException ex) {
                        Thread.currentThread().interrupt();
                    }
                }
            }
        }
    }

    private void handleVoucherOrder(VoucherOrder voucherOrder) {
        // 注意：这里是消费线程，UserHolder（ThreadLocal）里没有用户，
        // 所以 userId 必须从 voucherOrder 里取 —— 这就是消息里要带 userId 的原因。
        Long userId = voucherOrder.getUserId();
        RLock lock = redissonClient.getLock("lock:order:" + userId);
        boolean isLock = lock.tryLock();
        if (!isLock) {
            log.error("不允许重复下单！userId={}", userId);
            return;
        }
        try {
            // 从容器中取「自身的代理对象」来调用，保证 @Transactional 生效。
            // 不用 AopContext.currentProxy()：它依赖 ThreadLocal，消费线程里拿不到。
            applicationContext.getBean(IVoucherOrderService.class).createVoucherOrder(voucherOrder);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public Result seckillVoucher(Long voucherId) {
        // 1.获取用户
        Long userId = UserHolder.getUser().getId();

        // 2.生成订单id
        //   必须在执行 lua 之前生成，因为脚本要把它一起写进 Stream 消息里
        long orderId = redisIdWorker.nextId("order");

        // 3.执行 lua 脚本（原子：判断库存、判断一人一单、扣减库存、发送消息）
        Long result = stringRedisTemplate.execute(
                SECKILL_SCRIPT,
                Collections.emptyList(),
                voucherId.toString(),
                userId.toString(),
                String.valueOf(orderId)
        );

        // 4.判断结果是否为 0
        int r = result == null ? -1 : result.intValue();
        if (r != 0) {
            // 不为 0，代表没有购买资格：1=库存不足，2=不能重复下单
            return Result.fail(r == 1 ? "库存不足" : "不能重复下单");
        }

        // 5.为 0，说明抢购成功：消息已由脚本 XADD 进 Stream，
        //   后台消费者会异步落库，这里直接返回订单id，不用等数据库写完
        return Result.ok(orderId);
    }

    /**
     * 异步创建订单：扣减库存 + 保存订单（由后台消费线程调用）
     */
    @Transactional
    public void createVoucherOrder(VoucherOrder voucherOrder) {
        Long userId = voucherOrder.getUserId();
        Long voucherId = voucherOrder.getVoucherId();

        // 一人一单（Redis 中已判断过，这里作为数据库层的兜底校验）
        Long count = query().eq("user_id", userId).eq("voucher_id", voucherId).count();
        if (count > 0) {
            log.error("用户已经购买过一次！userId={}, voucherId={}", userId, voucherId);
            return;
        }

        // 扣减库存（乐观锁：stock > 0，防止数据库层超卖）
        boolean success = seckillVoucherService.update()
                .setSql("stock = stock - 1")
                .eq("voucher_id", voucherId).gt("stock", 0)
                .update();
        if (!success) {
            log.error("库存不足！voucherId={}", voucherId);
            return;
        }

        // 保存订单
        save(voucherOrder);
    }
}
