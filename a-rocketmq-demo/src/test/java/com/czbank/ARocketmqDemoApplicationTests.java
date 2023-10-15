package com.czbank;

import com.czbank.constant.MqConstant;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.List;
import java.util.UUID;

@SpringBootTest
class ARocketmqDemoApplicationTests {
    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Test
    void repeatProducer() throws Exception{
        DefaultMQProducer producer = new DefaultMQProducer("test-produce-group");
        //连接namesrv
        producer.setNamesrvAddr(MqConstant.NAME_SRV_ADDR);
        producer.start();
        String key = UUID.randomUUID().toString();

        Message message = new Message("repeatTopic",null,key,"repeatMessage".getBytes());
        Message message2 = new Message("repeatTopic",null,key,"repeatMessage".getBytes());
        producer.send(message);
        producer.send(message2);
        System.out.println("OK");
        //关闭生产者
        producer.shutdown();
    }
    @Test
    void repeatConsumer() throws Exception{
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("test-consumer-group");
        //连接namesrv
        consumer.setNamesrvAddr(MqConstant.NAME_SRV_ADDR);
        consumer.subscribe("repeatTopic","*");
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> list, ConsumeConcurrentlyContext consumeConcurrentlyContext) {
                MessageExt messageExt = list.get(0);
                String keys = messageExt.getKeys();
                try{

                    System.out.println(keys);
                    int i = jdbcTemplate.update("insert into mq.user(uuid,id,message) values (?,'1','123')",keys);
                    System.out.println(i);
                }catch (Exception e){
                    System.out.println("失败");
                }



                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });
        consumer.start();
        System.in.read();
    }


    @Test
    void contextLoads() throws Exception {
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

}
