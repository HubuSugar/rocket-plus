package edu.hubu.client.consumer.store;

import edu.hubu.client.instance.MQClientInstance;

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

    public MQClientInstance getMqClientInstance() {
        return mqClientInstance;
    }

    public String getGroupName() {
        return groupName;
    }
}
