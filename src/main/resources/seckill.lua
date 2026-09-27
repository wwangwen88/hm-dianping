-- 1.参数列表
-- 1.1.优惠券id
local voucherId = ARGV[1]
-- 1.2.用户id
local userId = ARGV[2]
-- 1.3.订单id
local orderId = ARGV[3]

-- 2.数据key
-- 2.1.库存key
local stockKey = 'seckill:stock:' .. voucherId
-- 2.2.订单key（Set，记录已下单的用户id，实现一人一单）
local orderKey = 'seckill:order:' .. voucherId

-- 3.脚本业务
-- Redis 执行 Lua 时是单线程的，下面「判断 + 扣减 + 发消息」整体原子，
-- 不会被其它请求穿插，因此不需要分布式锁也不会超卖、不会重复下单。

-- 3.1.判断库存是否充足
local stock = tonumber(redis.call('get', stockKey))
if (stock == nil or stock <= 0) then
    -- 库存不足，返回1
    return 1
end

-- 3.2.判断用户是否已经下过单 SISMEMBER orderKey userId
if (redis.call('sismember', orderKey, userId) == 1) then
    -- 已经下过单，重复下单，返回2
    return 2
end

-- 3.3.扣减库存 INCRBY stockKey -1
redis.call('incrby', stockKey, -1)
-- 3.4.记录下单用户 SADD orderKey userId
redis.call('sadd', orderKey, userId)
-- 3.5.发送消息到 Stream，消费者会异步读取并落库
--     XADD stream.orders * userId xx voucherId xx id xx
redis.call('xadd', 'stream.orders', '*', 'userId', userId, 'voucherId', voucherId, 'id', orderId)

-- 3.6.返回0，代表有购买资格
return 0
