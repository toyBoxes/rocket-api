package simple;

import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.nio.charset.StandardCharsets;
/*单向发送
* */
public class OneWayProducer {
    public static void main(String[] args) throws MQClientException, RemotingException, InterruptedException {
        DefaultMQProducer producer = new DefaultMQProducer("AsyncProducer");
        producer.setNamesrvAddr("192.168.1.29:9876");
        producer.setSendMsgTimeout(6000);//加上发送超时限制，不加报错
        producer.start();
        for (int i = 0; i < 10; i++) {
            Message message = new Message("Simple","TagA", "Simple-Oneway".getBytes(StandardCharsets.UTF_8));
            producer.sendOneway(message);
            System.out.printf("%d 消息发送完成 %n" , i);
        }
        Thread.sleep(5000);
        producer.shutdown();
    }
}
