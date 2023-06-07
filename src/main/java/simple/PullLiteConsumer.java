package simple;

import org.apache.rocketmq.client.consumer.DefaultLitePullConsumer;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.Arrays;
import java.util.List;

public class PullLiteConsumer {
    public static void main(String[] args) throws MQClientException {
        DefaultLitePullConsumer litePullConsumer = new DefaultLitePullConsumer("SimpleLitePullConsumer");
        litePullConsumer.setNamesrvAddr("192.168.1.29:9876");
        litePullConsumer.subscribe("Simple");
        litePullConsumer.start();
        while (true) {
            List<MessageExt> poll = litePullConsumer.poll();
            System.out.printf("消息拉取成功 %s%n" , poll);
            poll.forEach(n->{
                System.out.println("消息消费成功 " + new String(n.getBody()));
            });
        }
    }
}
