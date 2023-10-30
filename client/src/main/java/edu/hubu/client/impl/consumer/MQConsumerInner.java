package edu.hubu.client.impl.consumer;

import edu.hubu.common.message.MessageQueue;

import java.util.Set;

/**
 * @author: sugar
 * @date: 2023/10/28
 * @description:
 */
public interface MQConsumerInner {

    void doRebalance();

    boolean isUnitMode();

    void updateTopicSubscribeInfo(String topic, Set<MessageQueue> topicSubscribeInfo);
}
