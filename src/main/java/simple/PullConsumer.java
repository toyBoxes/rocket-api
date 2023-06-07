package simple;

import org.apache.rocketmq.client.consumer.DefaultMQPullConsumer;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.client.consumer.store.ReadOffsetType;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.util.HashSet;
import java.util.Set;

public class PullConsumer {
    public static void main(String[] args) throws MQClientException {
        DefaultMQPullConsumer pullConsumer = new DefaultMQPullConsumer("SimplePullConsumer");
        pullConsumer.setNamesrvAddr("192.168.1.29:9876");//执行nameserver地址
        Set<String> topics = new HashSet<>();
        topics.add("Simple");//添加Topic
        topics.add("TopicTest");
        pullConsumer.setRegisterTopics(topics);
        pullConsumer.start();
        while (true) { //循环拉取消息
            pullConsumer.getRegisterTopics().forEach(n -> {
                try {
                    Set<MessageQueue> messageQueues = pullConsumer.fetchSubscribeMessageQueues(n);//获取主题中的Queue
                    messageQueues.forEach(l -> {
                        try {
                            //获取Queue中的偏移量
                            long offset = pullConsumer.getOffsetStore().readOffset(l, ReadOffsetType.READ_FROM_MEMORY);
                            if (offset < 0) {
                                offset = pullConsumer.getOffsetStore().readOffset(l, ReadOffsetType.READ_FROM_STORE);
                            }
                            if (offset < 0) {
                                offset = pullConsumer.maxOffset(l);
                            }
                            if (offset < 0) {
                                offset = 0;
                            }
                            //拉取Queue中的消息。每次获取32条
                            PullResult pullResult = pullConsumer.pull(l, "*", offset, 32);
                            System.out.printf("循环拉取消息ing %s%n",pullResult);
                            switch (pullResult.getPullStatus()) {
                                case FOUND:
                                    pullResult.getMsgFoundList().forEach(p -> {
                                        System.out.printf("拉取消息成功%s%n", p);
                                    });
                                    //更新偏移量
                                    pullConsumer.updateConsumeOffset(l, pullResult.getNextBeginOffset());
                            }
                        } catch (MQClientException e) {
                            e.printStackTrace();
                        } catch (RemotingException e) {
                            e.printStackTrace();
                        } catch (MQBrokerException e) {
                            e.printStackTrace();
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    });
                } catch (MQClientException e) {
                    e.printStackTrace();
                }
            });
        }
    }
}
