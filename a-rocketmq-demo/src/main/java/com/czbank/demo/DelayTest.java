package com.czbank.demo;

import com.czbank.constant.MqConstant;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.junit.Test;

import java.util.Date;
import java.util.List;

public class DelayTest {

    @Test
    public void delayProducer() throws Exception{
        DefaultMQProducer producer = new DefaultMQProducer("delay-producer-group");
        producer.setNamesrvAddr(MqConstant.NAME_SRV_ADDR);
        producer.start();
        Message message = new Message("orderMsTopic","订单号，座位号xx".getBytes());
        message.setDelayTimeLevel(4);
        producer.send(message);
        System.out.println("发送时间"+new Date());
        producer.shutdown();
    }

    @Test
    public void msConsumer() throws Exception{
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("ms-consumer-group");
        consumer.setNamesrvAddr(MqConstant.NAME_SRV_ADDR);
        consumer.subscribe("orderMsTopic","*");
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> list, ConsumeConcurrentlyContext consumeConcurrentlyContext) {
                System.out.println("收到了"+new Date());
                System.out.println(new String(list.get(0).getBody()));
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });

        consumer.start();
        System.in.read();

    }
}
