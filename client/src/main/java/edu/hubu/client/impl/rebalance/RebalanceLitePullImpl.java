package edu.hubu.client.impl.rebalance;

import edu.hubu.client.consumer.MessageQueueListener;
import edu.hubu.client.consumer.store.ReadOffsetType;
import edu.hubu.client.consumer.strategy.AllocateMessageQueueStrategy;
import edu.hubu.client.exception.MQClientException;
import edu.hubu.client.impl.consumer.DefaultLitePullConsumerImpl;
import edu.hubu.client.impl.consumer.ProcessQueue;
import edu.hubu.client.impl.consumer.PullRequest;
import edu.hubu.client.instance.MQClientInstance;
import edu.hubu.common.consumer.ConsumeFromWhere;
import edu.hubu.common.message.MessageQueue;
import edu.hubu.common.protocol.heartbeat.ConsumeType;
import edu.hubu.common.protocol.heartbeat.MessageModel;
import edu.hubu.common.utils.MixAll;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Set;

/**
 * @author: sugar
 * @date: 2023/10/28
 * @description:
 */
@Slf4j
public class RebalanceLitePullImpl extends RebalanceImpl{

    private final DefaultLitePullConsumerImpl defaultLitePullConsumerImpl;


    public RebalanceLitePullImpl(final DefaultLitePullConsumerImpl litePullConsumer) {
        this(null, null, null, null, litePullConsumer);
    }

    public RebalanceLitePullImpl(String consumerGroup, MessageModel messageModel, AllocateMessageQueueStrategy allocateMessageQueueStrategy,
                                 MQClientInstance mqClientInstance, DefaultLitePullConsumerImpl litePullConsumer) {
        super(consumerGroup, messageModel, allocateMessageQueueStrategy, mqClientInstance);
        this.defaultLitePullConsumerImpl = litePullConsumer;
    }

    @Override
    public void messageQueueChanged(String topic, Set<MessageQueue> mqSet, Set<MessageQueue> allocateResultSet) {
        MessageQueueListener messageQueueListener = this.defaultLitePullConsumerImpl.getDefaultLitePullConsumer().getMessageQueueListener();
        if(messageQueueListener != null){
            try{
                messageQueueListener.messageQueueChanged(topic, mqSet, allocateResultSet);
            }catch (Throwable e){
                log.error(" message queue changed exception", e);
            }
        }
    }

    @Override
    public boolean removeUnnecessaryMessageQueue(MessageQueue mq, ProcessQueue pq) {
        // this.defaultLitePullConsumerImpl.getOffsetStore().persist(mq);
        // this.defaultLitePullConsumerImpl.getOffsetStore().removeOffset(mq);
        return true;
    }

    @Override
    public ConsumeType consumeType() {
        return ConsumeType.CONSUME_ACTIVELY;
    }

    @Override
    public void removeDirtyOffset(MessageQueue mq) {
        this.defaultLitePullConsumerImpl.getOffsetStore().removeOffset(mq);
    }

    @Override
    public long computePullFromWhere(MessageQueue mq) {
        ConsumeFromWhere consumeFromWhere = this.defaultLitePullConsumerImpl.getDefaultLitePullConsumer().getConsumeFromWhere();
        long result = -1;
        switch (consumeFromWhere){
            case CONSUME_FROM_LAST_OFFSET:{
                long lastOffset = defaultLitePullConsumerImpl.getOffsetStore().readOffset(mq,  ReadOffsetType.MEMORY_FIRST_THEN_STORE);
                if(lastOffset >= 0){
                    result = lastOffset;
                }else if(lastOffset == -1){
                    if(mq.getTopic().startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)){
                        result = 0L;
                    }else{
                        try{
                           result = this.mqClientInstance.getMqAdminImpl().maxOffset(mq);
                        }catch (MQClientException e){
                            result = -1;
                        }
                    }
                }else{
                    result = -1;
                }
                break;
            }
            case CONSUME_FROM_FIRST_OFFSET:
                break;
        }
        return result;
    }

    @Override
    public void dispatchPullRequest(List<PullRequest> pullRequests) {

    }
}
