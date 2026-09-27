package com.hmdp.utils;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest
public class NextIdTest {

    @Autowired
    private RedisIdWorker redisIdWorker;

    @Test
    void testNextId() {
        // 连续生成 10 个 ID 并打印
        for (int i = 1; i <= 10; i++) {
            long nextId = redisIdWorker.nextId("order");
            System.out.println("第 " + i + " 个订单ID: " + nextId);
        }
    }
}
