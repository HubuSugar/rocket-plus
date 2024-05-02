package edu.hubu.store;

import edu.hubu.common.message.Message;
import edu.hubu.common.message.MessageExtBrokerInner;

import java.util.concurrent.CompletableFuture;

/**
 * @author: sugar
 * @date: 2023/7/15
 * @description:
 */
public interface MessageStore {

    boolean load();

    void start() throws Exception;

    default CompletableFuture<PutMessageResult> asyncPutMessage(final MessageExtBrokerInner message){
        return CompletableFuture.completedFuture(putMessage(message));
    }

    PutMessageResult putMessage(final MessageExtBrokerInner message);

    void cleanExpiredConsumeQueue();

    long getMaxOffsetInQueue(String topic, Integer queueId);

    /**
     * Query at most <code>maxMsgNums</code>
     * @param consumerGroup 消费者组
     * @param topic 主题
     * @param queueId 队列id
     * @param queueOffset 队列偏移量
     * @param maxMsgNums 最大消息数量
     * @param messageFilter 消息过滤
     * @return
     */
    GetMessageResult getMessage(String consumerGroup, String topic, Integer queueId, Long queueOffset, Integer maxMsgNums, MessageFilter messageFilter);
}
