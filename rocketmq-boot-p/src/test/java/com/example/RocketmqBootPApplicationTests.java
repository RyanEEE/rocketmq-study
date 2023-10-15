package com.example;

import lombok.val;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.spring.core.RocketMQTemplate;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;

@SpringBootTest
class RocketmqBootPApplicationTests {
    @Autowired
    RocketMQTemplate rocketMQTemplate;
    @Test
    void contextLoads() {
        rocketMQTemplate.syncSend("bootTestTopic","boot同步消息");
        rocketMQTemplate.syncSend("bootTestTopic","boot同步消息22");
        rocketMQTemplate.asyncSend("bootTestTopic", "异步boot消息", new SendCallback() {
            @Override
            public void onSuccess(SendResult sendResult) {
                System.out.println("成功");
            }

            @Override
            public void onException(Throwable throwable) {
                System.out.println("失败");
            }
        });

        rocketMQTemplate.sendOneWay("bootOnWayTopic","OneWay");
        Message<String> msg = MessageBuilder.withPayload("延迟消息").build();
        rocketMQTemplate.syncSend("bootMsTopic",msg,3000,3);


    }

}
