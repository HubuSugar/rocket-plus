package edu.hubu.client.consumer.store;

import edu.hubu.client.instance.MQClientInstance;
import edu.hubu.common.message.MessageQueue;
import edu.hubu.common.utils.MixAll;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author: sugar
 * @date: 2023/10/29
 * @description:
 */
public class LocalFileOffsetStore implements OffsetStore{

    private final MQClientInstance mqClientInstance;
    private final String groupName;
    private final ConcurrentHashMap<MessageQueue, AtomicLong> offsetTable = new ConcurrentHashMap<>();

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

    @Override
    public void updateOffset(MessageQueue messageQueue, long consumerOffset, boolean increaseOnly) {
        if(messageQueue != null){
            AtomicLong offset = this.offsetTable.get(messageQueue);
            if(offset == null){
                offset = this.offsetTable.putIfAbsent(messageQueue, new AtomicLong(consumerOffset));
            }

            if(offset != null){
                if(increaseOnly){
                    MixAll.compareAndIncreaseOnly(offset, consumerOffset);
                }else{
                    offset.set(consumerOffset);
                }
            }

        }
    }

    public MQClientInstance getMqClientInstance() {
        return mqClientInstance;
    }

    public String getGroupName() {
        return groupName;
    }
}
