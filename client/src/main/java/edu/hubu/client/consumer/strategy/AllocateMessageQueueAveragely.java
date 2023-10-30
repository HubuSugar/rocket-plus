package edu.hubu.client.consumer.strategy;

import edu.hubu.common.message.MessageQueue;

import java.util.List;

/**
 * @author: sugar
 * @date: 2023/10/28
 * @description:
 */
public class AllocateMessageQueueAveragely implements AllocateMessageQueueStrategy {

    @Override
    public List<MessageQueue> allocate(String consumerGroup, String clientId, List<MessageQueue> mqSet, List<String> cidAll) {

        return null;
    }

    @Override
    public String getName() {
        return AllocateMessageQueueStrategy.class.getSimpleName();
    }
}
