package order;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.*;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.List;

public class OrderConsumer {

    public static void main(String[] args) throws MQClientException {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("OrderConsumer");
        consumer.setNamesrvAddr("192.168.1.29:9876");
        consumer.subscribe("OrderTopic","*");
        consumer.setMessageListener(new MessageListenerOrderly() {//顺序消费
            @Override
            public ConsumeOrderlyStatus consumeMessage(List<MessageExt> list, ConsumeOrderlyContext consumeOrderlyContext) {
                list.forEach(n->{
                    System.out.println("QueueId:"+n.getQueueId() + "收到消息内容 "+new String(n.getBody()));
                });
                return ConsumeOrderlyStatus.SUCCESS;
            }
        });
        /*consumer.setMessageListener(new MessageListenerConcurrently() {//并发消费
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> list, ConsumeConcurrentlyContext consumeConcurrentlyContext) {
                list.forEach(n->{
                    System.out.println("QueueId:"+n.getQueueId() + "收到消息内容 "+new String(n.getBody()));
                });
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });*/
        consumer.start();
        System.out.printf("Consumer Started.%n");
    }
}
