package edu.hubu.client.consumer.store;

import edu.hubu.client.instance.MQClientInstance;
import edu.hubu.common.message.MessageQueue;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author: sugar
 * @date: 2023/10/29
 * @description:
 */
public class RemoteBrokerOffsetStore implements OffsetStore{

    private final MQClientInstance mqClientInstance;
    private final String groupName;
    private final ConcurrentHashMap<MessageQueue, AtomicLong> offsetTable = new ConcurrentHashMap<>();

    public RemoteBrokerOffsetStore(MQClientInstance mqClientInstance, String groupName) {
        this.mqClientInstance = mqClientInstance;
        this.groupName = groupName;
    }

    @Override
    public void load() {

    }

    @Override
    public long readOffset(MessageQueue mq, ReadOffsetType readOffsetType) {
        if(mq != null){
            switch (readOffsetType){
                case READ_FROM_STORE:
                    break;
                case MEMORY_FIRST_THEN_STORE:
                case READ_FROM_MEMORY:
                    AtomicLong offset = this.offsetTable.get(mq);
                    if(offset != null){
                        return offset.get();
                    }else if(ReadOffsetType.READ_FROM_MEMORY == readOffsetType){
                        return -1;
                    }
            }
        }
        return -1;
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
