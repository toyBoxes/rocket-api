package simple;

import org.apache.rocketmq.client.consumer.DefaultLitePullConsumer;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

public class PullLiteConsumerAssign {
    public static void main(String[] args) throws Exception {
        DefaultLitePullConsumer litePullConsumer = new DefaultLitePullConsumer("SimpleLitePullConsumer");
        litePullConsumer.setNamesrvAddr("192.168.1.29:9876");
        litePullConsumer.start();
        Collection<MessageQueue> messageQueues = litePullConsumer.fetchMessageQueues("TopicTest");
        List<MessageQueue> list = new ArrayList<>(messageQueues);
        litePullConsumer.assign(list);
        litePullConsumer.seek(list.get(0), 10);
        try {
            while (true) {
                List<MessageExt> messageExts = litePullConsumer.poll();
                System.out.println("消息拉取成功:" );
                messageExts.forEach(n->{
                    System.out.println("消息消费成功: " + n);
                });

            }
        } finally {
            litePullConsumer.shutdown();
        }
    }
}
