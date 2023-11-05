package edu.hubu.common.protocol.body;

import edu.hubu.common.message.MessageQueue;
import edu.hubu.remoting.netty.protocol.RemotingSerialize;

import java.util.HashSet;
import java.util.Set;

/**
 * @author: sugar
 * @date: 2023/11/2
 * @description:
 */
public class LockBatchRequestBody extends RemotingSerialize {
    private String consumerGroup;
    private String clientId;
    private Set<MessageQueue> mqSet = new HashSet<>();

    public String getConsumerGroup() {
        return consumerGroup;
    }

    public void setConsumerGroup(String consumerGroup) {
        this.consumerGroup = consumerGroup;
    }

    public String getClientId() {
        return clientId;
    }

    public void setClientId(String clientId) {
        this.clientId = clientId;
    }

    public Set<MessageQueue> getMqSet() {
        return mqSet;
    }

    public void setMqSet(Set<MessageQueue> mqSet) {
        this.mqSet = mqSet;
    }
}
