package com.hmdp;

import com.hmdp.entity.SeckillVoucher;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.utils.RedisConstants;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.core.StringRedisTemplate;

import javax.annotation.Resource;
import java.util.List;

/**
 * 秒杀库存预热：把数据库里的秒杀券库存同步到 Redis（seckill:stock:{voucherId}）。
 * 正式跑秒杀前执行一次即可；否则 Lua 脚本取不到库存，会一律返回「库存不足」。
 */
@SpringBootTest
class SeckillStockWarmUpTest {

    @Resource
    private ISeckillVoucherService seckillVoucherService;
    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Test
    void warmUpStock() {
        List<SeckillVoucher> list = seckillVoucherService.list();
        if (list.isEmpty()) {
            System.out.println("数据库中没有秒杀券，请先调用 /voucher/seckill 新增一张");
            return;
        }
        for (SeckillVoucher sv : list) {
            String key = RedisConstants.SECKILL_STOCK_KEY + sv.getVoucherId();
            stringRedisTemplate.opsForValue().set(key, String.valueOf(sv.getStock()));
            System.out.println("预热 " + key + " = " + sv.getStock());
        }
    }
}
