package com.czbank.demo;

import com.czbank.constant.MqConstant;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.junit.Test;

import java.util.List;

public class ASimpleTest {
    @Test
    public void contextLoads() throws Exception {
        DefaultMQProducer producer = new DefaultMQProducer("test-produce-group");
        //连接namesrv
        producer.setNamesrvAddr(MqConstant.NAME_SRV_ADDR);
        producer.start();
        Message message = new Message("testTopic","我是一个简单消息".getBytes());
        SendResult sendResult = producer.send(message);
        System.out.println(sendResult.getSendStatus());
        //关闭生产者
        producer.shutdown();
    }

    //消费者
    @Test
    public void simpleConsumer() throws Exception {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("test-consumer-group");
        consumer.setNamesrvAddr(MqConstant.NAME_SRV_ADDR);
        // 订阅所有消息*
        consumer.subscribe("testTopic","*");
        //监听器
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> list, ConsumeConcurrentlyContext consumeConcurrentlyContext) {
                //业务处理
                System.out.println("我是消费者");
                System.out.println(new String(list.get(0).getBody()));
                System.out.println("消费上下文"+consumeConcurrentlyContext);
                //成功，消息出队列， 反之~ 重新消费
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });

        consumer.start();

        //挂起jvm
        System.in.read();
    }
}
