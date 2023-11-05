package edu.hubu.client.consumer.store;

import edu.hubu.client.instance.MQClientInstance;
import edu.hubu.common.message.MessageQueue;

/**
 * @author: sugar
 * @date: 2023/10/29
 * @description:
 */
public class LocalFileOffsetStore implements OffsetStore{

    private final MQClientInstance mqClientInstance;
    private final String groupName;

    public LocalFileOffsetStore(MQClientInstance mqClientInstance, String groupName) {
        this.mqClientInstance = mqClientInstance;
        this.groupName = groupName;
    }

    @Override
    public void load() {

    }

    @Override
    public long readOffset(MessageQueue mq, ReadOffsetType readOffsetType) {
        return 0;
    }

    @Override
    public void removeOffset(MessageQueue mq) {

    }

    public MQClientInstance getMqClientInstance() {
        return mqClientInstance;
    }

    public String getGroupName() {
        return groupName;
    }
}
