package edu.hubu.client.impl.consumer;

import edu.hubu.common.message.MessageQueue;

import java.util.concurrent.ConcurrentHashMap;

/**
 * @author: sugar
 * @date: 2024/6/2
 * @description:
 */
public class MessageQueueLock {
    private final ConcurrentHashMap<MessageQueue, Object> mqLockTable = new ConcurrentHashMap<>();

    public Object fetchLockObject(MessageQueue mq){
        Object lockObj = this.mqLockTable.get(mq);
        if(lockObj == null){
            lockObj = new Object();
            Object prev = this.mqLockTable.putIfAbsent(mq, lockObj);
            if(prev != null){
                lockObj = prev;
            }
        }
        return lockObj;
    }

}
