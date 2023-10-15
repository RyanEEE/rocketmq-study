package com.czbank.demo;

import com.czbank.constant.MqConstant;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.junit.Test;

public class AsyncTest {
    @Test
    public void asyncProducer() throws Exception {
        DefaultMQProducer producer = new DefaultMQProducer("async-produce-group");
        //连接namesrv
        producer.setNamesrvAddr(MqConstant.NAME_SRV_ADDR);
        producer.start();
        Message message = new Message("testTopic","我是一个异步消息".getBytes());
        producer.send(message, new SendCallback() {
            @Override
            public void onSuccess(SendResult sendResult) {
                System.out.println("successful");
            }

            @Override
            public void onException(Throwable throwable) {
                System.err.println("error");
            }
        });
        System.out.println("first");
        System.in.read();
    }
}
