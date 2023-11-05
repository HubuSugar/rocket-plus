package edu.hubu.client.consumer.store;

import edu.hubu.common.message.MessageQueue;

/**
 * @author: sugar
 * @date: 2023/10/29
 * @description:
 */
public interface OffsetStore {

    void load();

    long readOffset(MessageQueue mq, ReadOffsetType readOffsetType);

    void removeOffset(MessageQueue mq);

}
