package com.example.listener;

import org.apache.rocketmq.spring.annotation.ConsumeMode;
import org.apache.rocketmq.spring.annotation.RocketMQMessageListener;
import org.apache.rocketmq.spring.core.RocketMQListener;
import org.springframework.stereotype.Component;

@Component
@RocketMQMessageListener(topic="bootTestTopic",consumerGroup = "boot-test-consumer-group",
consumeMode = ConsumeMode.ORDERLY,
maxReconsumeTimes = 5)
public class ABootSimpleMsgListener implements RocketMQListener<String> {
    @Override
    public void onMessage(String s) {
        System.out.println(s);
    }
}
