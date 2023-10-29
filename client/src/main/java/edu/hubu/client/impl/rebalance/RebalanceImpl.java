package edu.hubu.client.impl.rebalance;

import edu.hubu.client.consumer.strategy.AllocateMessageQueueStrategy;
import edu.hubu.client.instance.MQClientInstance;
import edu.hubu.common.protocol.heartbeat.MessageModel;

/**
 * @author: sugar
 * @date: 2023/10/28
 * @description:
 */
public abstract class RebalanceImpl {

    protected String consumerGroup;
    protected MessageModel messageModel;
    protected AllocateMessageQueueStrategy allocateMessageQueueStrategy;
    protected MQClientInstance mqClientInstance;

    public RebalanceImpl(String consumerGroup, MessageModel messageModel, AllocateMessageQueueStrategy allocateMessageQueueStrategy, MQClientInstance mqClientInstance) {
        this.consumerGroup = consumerGroup;
        this.messageModel = messageModel;
        this.allocateMessageQueueStrategy = allocateMessageQueueStrategy;
        this.mqClientInstance = mqClientInstance;
    }

    public String getConsumerGroup() {
        return consumerGroup;
    }

    public void setConsumerGroup(String consumerGroup) {
        this.consumerGroup = consumerGroup;
    }

    public MessageModel getMessageModel() {
        return messageModel;
    }

    public void setMessageModel(MessageModel messageModel) {
        this.messageModel = messageModel;
    }

    public AllocateMessageQueueStrategy getAllocateMessageQueueStrategy() {
        return allocateMessageQueueStrategy;
    }

    public void setAllocateMessageQueueStrategy(AllocateMessageQueueStrategy allocateMessageQueueStrategy) {
        this.allocateMessageQueueStrategy = allocateMessageQueueStrategy;
    }

    public MQClientInstance getMqClientInstance() {
        return mqClientInstance;
    }

    public void setMqClientInstance(MQClientInstance mqClientInstance) {
        this.mqClientInstance = mqClientInstance;
    }
}
