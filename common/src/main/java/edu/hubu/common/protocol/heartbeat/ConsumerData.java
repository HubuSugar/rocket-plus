package edu.hubu.common.protocol.heartbeat;

import edu.hubu.common.consumer.ConsumeFromWhere;

import java.util.HashSet;
import java.util.Set;

/**
 * @author: sugar
 * @date: 2024/7/21
 * @description:
 */
public class ConsumerData {
    private String groupName;
    private ConsumeType consumerType;
    private MessageModel messageModel;
    private ConsumeFromWhere consumeFromWhere;
    private Set<SubscriptionData> subscriptionData = new HashSet<>();
    private boolean unitMode;

    public String getGroupName() {
        return groupName;
    }

    public void setGroupName(String groupName) {
        this.groupName = groupName;
    }

    public ConsumeType getConsumerType() {
        return consumerType;
    }

    public void setConsumerType(ConsumeType consumerType) {
        this.consumerType = consumerType;
    }

    public MessageModel getMessageModel() {
        return messageModel;
    }

    public void setMessageModel(MessageModel messageModel) {
        this.messageModel = messageModel;
    }

    public ConsumeFromWhere getConsumeFromWhere() {
        return consumeFromWhere;
    }

    public void setConsumeFromWhere(ConsumeFromWhere consumeFromWhere) {
        this.consumeFromWhere = consumeFromWhere;
    }

    public Set<SubscriptionData> getSubscriptionData() {
        return subscriptionData;
    }

    public void setSubscriptionData(Set<SubscriptionData> subscriptionData) {
        this.subscriptionData = subscriptionData;
    }

    public boolean isUnitMode() {
        return unitMode;
    }

    public void setUnitMode(boolean unitMode) {
        this.unitMode = unitMode;
    }
}
