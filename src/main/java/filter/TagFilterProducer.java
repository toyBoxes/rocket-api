package filter;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.nio.charset.StandardCharsets;

public class TagFilterProducer {
    public static void main(String[] args) throws MQClientException, MQBrokerException, RemotingException, InterruptedException {
        DefaultMQProducer producer = new DefaultMQProducer("SyncProducer");
        producer.setNamesrvAddr("192.168.1.29:9876");
        producer.setSendMsgTimeout(6000);
        producer.start();
        String[] tags = new String[] {"TagA","TagB","TagC"};
        for (int i = 0; i < 15; i++) {
            Message msg = new Message("FilterTopic", //主题
                    tags[i % tags.length],  //设置消息Tag，用于消费端根据指定Tag过滤消息。
                    ("TagFilterProducer_"+tags[i % tags.length]).getBytes(StandardCharsets.UTF_8) //消息体。
            );
            SendResult send = producer.send(msg);
            System.out.printf(i + ".发送消息成功：%s%n", send);
        }
        producer.shutdown();
    }
}
