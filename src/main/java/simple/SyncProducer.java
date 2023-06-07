package simple;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.nio.charset.StandardCharsets;

/*
* 同步发送消息
* */
public class SyncProducer {
    public static void main(String[] args) throws MQClientException, MQBrokerException, RemotingException, InterruptedException {
        DefaultMQProducer producer = new DefaultMQProducer("SyncProducer");
        producer.setNamesrvAddr("192.168.1.29:9876");
        producer.setSendMsgTimeout(5000);
        producer.start();
        for (int i = 0; i < 2; i++) {
            Message msg = new Message("Simple", //主题
                    "TagA",  //设置消息Tag，用于消费端根据指定Tag过滤消息。
                    (i+"Simple-Sync").getBytes(StandardCharsets.UTF_8) //消息体。
            );
            SendResult send = producer.send(msg);
            System.out.printf(i + ".发送消息成功：%s%n", send);
        }
        producer.shutdown();
    }

}
