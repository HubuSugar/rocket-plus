package edu.hubu.common.protocol.topic;

import edu.hubu.common.message.MessageQueue;

/**
 * @author: sugar
 * @date: 2024/5/26
 * @description:
 */
public class OffsetMovedEvent {
    private String consumerGroup;
    private MessageQueue messageQueue;
    private Long offsetRequest;
    private Long offsetNew;

    public String getConsumerGroup() {
        return consumerGroup;
    }

    public void setConsumerGroup(String consumerGroup) {
        this.consumerGroup = consumerGroup;
    }

    public MessageQueue getMessageQueue() {
        return messageQueue;
    }

    public void setMessageQueue(MessageQueue messageQueue) {
        this.messageQueue = messageQueue;
    }

    public Long getOffsetRequest() {
        return offsetRequest;
    }

    public void setOffsetRequest(Long offsetRequest) {
        this.offsetRequest = offsetRequest;
    }

    public Long getOffsetNew() {
        return offsetNew;
    }

    public void setOffsetNew(Long offsetNew) {
        this.offsetNew = offsetNew;
    }
}
