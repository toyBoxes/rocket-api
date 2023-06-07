package order;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.nio.charset.StandardCharsets;
import java.util.List;

public class OrderProducer {
    public static void main(String[] args) throws MQClientException, MQBrokerException, RemotingException, InterruptedException {
        DefaultMQProducer producer=new DefaultMQProducer("OrderProducer");
        producer.setNamesrvAddr("192.168.1.29:9876");
        producer.setSendMsgTimeout(6000);
        producer.start();
        for (int j = 0; j < 5; j++) {//顺序发送生产者
            for (int i = 0; i < 10; i++) {
                Message message = new Message("OrderTopic","TagA",
                        ("order_" + j + "_step_" + i).getBytes(StandardCharsets.UTF_8));
                SendResult sendResult = producer.send(message, new MessageQueueSelector() {
                    @Override
                    public MessageQueue select(List<MessageQueue> list, Message message, Object o) {
                        Integer id = (Integer) o;
                        int index = id % list.size();
                        return list.get(index);
                    }
                }, j);
                System.out.printf("%s%n", sendResult);
            }
        }
        producer.shutdown();
    }
}
