package edu.hubu.client.consumer.strategy;

import edu.hubu.common.message.MessageQueue;

import java.util.List;

/**
 * @author: sugar
 * @date: 2023/10/28
 * @description:
 */
public interface AllocateMessageQueueStrategy {

    List<MessageQueue> allocate(final String consumerGroup, final String clientId, List<MessageQueue> mqSet, List<String> cidAll);

    String getName();
}
