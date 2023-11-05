package edu.hubu.client.impl.consumer;

import edu.hubu.common.message.MessageQueue;
import lombok.EqualsAndHashCode;
import lombok.extern.slf4j.Slf4j;

/**
 * @author: sugar
 * @date: 2023/10/29
 * @description:
 */
@Slf4j
@EqualsAndHashCode
public class PullRequest {
    private String consumerGroup;
    private long nextOffset;
    private MessageQueue messageQueue;
    private ProcessQueue processQueue;
    private boolean lockFirst;

    public String getConsumerGroup() {
        return consumerGroup;
    }

    public void setConsumerGroup(String consumerGroup) {
        this.consumerGroup = consumerGroup;
    }

    public long getNextOffset() {
        return nextOffset;
    }

    public void setNextOffset(long nextOffset) {
        this.nextOffset = nextOffset;
    }

    public MessageQueue getMessageQueue() {
        return messageQueue;
    }

    public void setMessageQueue(MessageQueue messageQueue) {
        this.messageQueue = messageQueue;
    }

    public ProcessQueue getProcessQueue() {
        return processQueue;
    }

    public void setProcessQueue(ProcessQueue processQueue) {
        this.processQueue = processQueue;
    }

    public boolean isLockFirst() {
        return lockFirst;
    }

    public void setLockFirst(boolean lockFirst) {
        this.lockFirst = lockFirst;
    }
}
