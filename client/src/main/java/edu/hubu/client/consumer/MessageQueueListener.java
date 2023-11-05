package edu.hubu.client.consumer;

import edu.hubu.common.message.MessageQueue;

import java.util.Set;

/**
 * @author: sugar
 * @date: 2023/10/31
 * @description:
 */
public interface MessageQueueListener {

    void messageQueueChanged(final String topic, final Set<MessageQueue> mqAll, final Set<MessageQueue> mqDivided);

}
