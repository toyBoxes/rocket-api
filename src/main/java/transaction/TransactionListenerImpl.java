package transaction;

import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.producer.LocalTransactionState;
import org.apache.rocketmq.client.producer.TransactionListener;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;

public class TransactionListenerImpl implements TransactionListener {
    @Override
    /**
     * 在提交完事务消息后执行。
     * 返回COMMIT_MESSAGE状态的消息会立即被消费者消费到。
     * 返回ROLLBACK_MESSAGE状态的消息会被丢弃。
     * 返回UNKNOWN状态的消息会由Broker过一段时间再来回查事务的状态。
     */
    public LocalTransactionState executeLocalTransaction(Message message, Object o) {
        String tags = message.getTags();
        //TagA的消息会立即被消费者消费到
        if(StringUtils.contains(tags,"TagA")){
            return LocalTransactionState.COMMIT_MESSAGE;
            //TagB的消息会被丢弃
        }else if(StringUtils.contains(tags,"TagB")){
            return LocalTransactionState.ROLLBACK_MESSAGE;
            //其他消息会等待Broker进行事务状态回查。
        }else{
            return LocalTransactionState.UNKNOW;
        }
    }
    @Override
    /**
     * 在对UNKNOWN状态的消息进行状态回查时执行。
     * 返回COMMIT_MESSAGE状态的消息会立即被消费者消费到。
     * 返回ROLLBACK_MESSAGE状态的消息会被丢弃。
     * 返回UNKNOWN状态的消息会由Broker过一段时间再来回查事务的状态。
     */
    public LocalTransactionState checkLocalTransaction(MessageExt messageExt) {
        String tags = messageExt.getTags();
        //TagC的消息过一段时间会被消费者消费到
        if(StringUtils.contains(tags,"TagC")){
            return LocalTransactionState.COMMIT_MESSAGE;
            //TagD的消息也会在状态回查时被丢弃掉
        }else if(StringUtils.contains(tags,"TagD")){
            return LocalTransactionState.ROLLBACK_MESSAGE;
            //剩下TagE的消息会在多次状态回查后最终丢弃
        }else{
            return LocalTransactionState.UNKNOW;
        }
    }
}
