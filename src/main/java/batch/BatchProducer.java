package batch;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;

public class BatchProducer {

    public static void main(String[] args) throws MQClientException, MQBrokerException, RemotingException, InterruptedException {
        DefaultMQProducer producer = new DefaultMQProducer("BatchProducer");
        producer.setNamesrvAddr("192.168.1.29:9876");
        producer.setSendMsgTimeout(6000);
        producer.start();
        ArrayList<Message> messages = new ArrayList<>();
        messages.add(new Message("Simple","TagA", "BatchProducer0".getBytes(StandardCharsets.UTF_8)));
        messages.add(new Message("Simple","TagA", "BatchProducer1".getBytes(StandardCharsets.UTF_8)));
        messages.add(new Message("Simple","TagA", "BatchProducer2".getBytes(StandardCharsets.UTF_8)));
        SendResult send = producer.send(messages);
        System.out.printf(".发送消息成功：%s%n", send);
        producer.shutdown();
    }
}
