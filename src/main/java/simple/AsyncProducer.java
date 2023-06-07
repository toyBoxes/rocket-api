package simple;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
/*
* 异步发送
* */
public class AsyncProducer {

    public static void main(String[] args) throws MQClientException, MQBrokerException, RemotingException, InterruptedException {
        DefaultMQProducer producer = new DefaultMQProducer("AsyncProducer");
        producer.setNamesrvAddr("192.168.1.29:9876");
        producer.start();
        producer.setSendMsgTimeout(6000);
        CountDownLatch countDownLatch = new CountDownLatch(100);//计数
        for (int i = 0; i < 100; i++) {
            Message message = new Message("Simple", "TagA", (i+"Simple-Async").getBytes(StandardCharsets.UTF_8));
            final int index = i;
            producer.send(message, new SendCallback() {
                        @Override
                        public void onSuccess(SendResult sendResult) {
                            countDownLatch.countDown();
                            System.out.printf("%d 消息发送成功%s%n", index, sendResult);
                        }

                        @Override
                        public void onException(Throwable throwable) {
                            countDownLatch.countDown();
                            System.out.printf("%d 消息失败%s%n", index, throwable);
                            throwable.printStackTrace();
                        }
                    }
            );
        }
        countDownLatch.await(5, TimeUnit.SECONDS);
        producer.shutdown();
    }
}
