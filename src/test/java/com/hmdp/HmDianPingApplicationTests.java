package com.hmdp;

import com.hmdp.utils.RedisIdWorker;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import javax.annotation.Resource;

@SpringBootTest
class HmDianPingApplicationTests {

    @Resource
    private RedisIdWorker redisIdWorker;

    @Test
    void testNextId() {
        for (int i = 0; i < 5; i++) {
            long id = redisIdWorker.nextId("order");
            System.out.println("id = " + id);
        }
    }

}
