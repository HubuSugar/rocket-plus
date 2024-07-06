package edu.hubu.client.consumer;

import edu.hubu.common.message.MessageQueue;

import java.util.Set;

/**
 * @author: sugar
 * @date: 2024/6/15
 * @description:
 */
public interface TopicMessageQueueChangeListener {

    /**
     * This method will be invoked in the condition of queue numbers changed, These scenarios occur when the topic is expanded or shrunk
     * @param topic
     * @param messageQueues
     */
    void onChanged(String topic, Set<MessageQueue> messageQueues);
}
