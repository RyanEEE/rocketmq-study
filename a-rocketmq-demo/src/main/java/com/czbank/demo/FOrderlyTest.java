package com.czbank.demo;
import com.czbank.constant.MqConstant;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.*;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.junit.Test;

import java.util.Arrays;
import java.util.Date;
import java.util.List;
public class FOrderlyTest {
    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    class Student {
        private int id;
        private String name;
    }

    @Test
    public void delayProducer() throws Exception{
        DefaultMQProducer producer = new DefaultMQProducer("FOrder-producer-group");
        producer.setNamesrvAddr(MqConstant.NAME_SRV_ADDR);
        List<Student> students = Arrays.asList(
                new Student(1,"xx"),
                new Student(1,"yy"),
                new Student(1,"zz"),
                new Student(2,"aa"),
                new Student(2,"bb"),
                new Student(2,"cc"),
                new Student(3,"xx"),
                new Student(3,"dd")
        );

        producer.start();
        students.forEach(student -> {
            Message message = new Message("orderTopic",student.toString().getBytes());
            try{
                producer.send(message, new MessageQueueSelector() {
                    @Override
                    public MessageQueue select(List<MessageQueue> list, Message message, Object o) {
                        int hashcode = o.toString().hashCode();
                        int i = hashcode % list.size();
                        return list.get(i);
                    }
                }, student.getId());
            }catch (Exception e){
                e.printStackTrace();
            }
        });

        System.out.println("发送时间"+new Date());
        producer.shutdown();
    }

    @Test
    public void msConsumer() throws Exception{
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("order-consumer-group");
        consumer.setNamesrvAddr(MqConstant.NAME_SRV_ADDR);
        consumer.subscribe("orderTopic","*");
        consumer.registerMessageListener(new MessageListenerOrderly() {
            @Override
            public ConsumeOrderlyStatus consumeMessage(List<MessageExt> list, ConsumeOrderlyContext consumeConcurrentlyContext) {
                System.out.println("thread"+Thread.currentThread().getId());
                System.out.println(new String(list.get(0).getBody()));
                return ConsumeOrderlyStatus.SUCCESS;
            }
        });

        consumer.start();
        System.in.read();

    }
}
