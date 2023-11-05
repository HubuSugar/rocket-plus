package edu.hubu.broker.client.rebalance;

import edu.hubu.common.message.MessageQueue;
import lombok.extern.slf4j.Slf4j;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author: sugar
 * @date: 2023/11/5
 * @description:
 */
@Slf4j
public class RebalanceLockManager {

    private static final long REBALANCE_LOCK_MAX_IDLE_TIME = Long.parseLong(System.getProperty("rocketmq.broker.rebalance.lockMaxIdleTime", "60000"));
    private final Lock lock = new ReentrantLock();
    private final ConcurrentHashMap<String, ConcurrentHashMap<MessageQueue, LockEntry>> mqLockTable = new ConcurrentHashMap<>(1024);


    private boolean isLocked(String consumerGroup, MessageQueue messageQueue, String clientId){
        ConcurrentHashMap<MessageQueue, LockEntry> lockEntryMap = this.mqLockTable.get(consumerGroup);
        if(lockEntryMap != null){
            LockEntry lockEntry = lockEntryMap.get(messageQueue);
            if(lockEntry != null){
               boolean locked =  lockEntry.isLocked(clientId);
               if(locked){
                   lockEntry.setLastUpdateTimestamp(System.currentTimeMillis());
               }
               return locked;
            }
        }
        return false;
    }

    public Set<MessageQueue> tryLockBatch(final String consumerGroup, final String clientId, Set<MessageQueue> mqSet){
        Set<MessageQueue> lockedMq = new HashSet<>();
        Set<MessageQueue> unlockedMq = new HashSet<>();

        for (MessageQueue mq : mqSet) {
            if(this.isLocked(consumerGroup, mq, clientId)){
                lockedMq.add(mq);
            }else{
                unlockedMq.add(mq);
            }
        }

        if(!unlockedMq.isEmpty()){
            try {
                this.lock.lockInterruptibly();
                try{
                    ConcurrentHashMap<MessageQueue, LockEntry> lockEntryMap = this.mqLockTable.get(consumerGroup);
                    if(lockEntryMap == null){
                        lockEntryMap = new ConcurrentHashMap<>(32);
                        this.mqLockTable.put(consumerGroup, lockEntryMap);
                    }

                    for (MessageQueue mq : unlockedMq) {
                        LockEntry lockEntry = lockEntryMap.get(mq);

                        if(lockEntry == null){
                            lockEntry = new LockEntry();
                            lockEntry.setClientId(clientId);
                            lockEntryMap.put(mq, lockEntry);
                            log.warn("try lock batch, message queue not locked, consumerGroup:{}, new clientId:{}, messageQueue:{}", consumerGroup, clientId, mq);
                        }

                        //再次判断，如果被锁定了，加入到锁定mq集合
                        if(lockEntry.isLocked(clientId)){
                            lockEntry.setLastUpdateTimestamp(System.currentTimeMillis());
                            lockedMq.add(mq);
                            continue;
                        }

                        //再次判断锁定mq是否超期
                        String oldClientId = lockEntry.getClientId();
                        if(lockEntry.isExpired()){
                            lockEntry.setClientId(clientId);
                            lockEntry.setLastUpdateTimestamp(System.currentTimeMillis());
                            log.warn("tryLockBatch message queue lock expired, consumerGroup:{}, oldClientId:{}, messageQueue:{}", consumerGroup, oldClientId, mq);
                            continue;
                        }

                        log.warn("tryLockBatch message queue locked by other client, consumerGroup:{}, oldClientId:{}, newClientId:{}, messageQueue:{}", consumerGroup, oldClientId, clientId, mq);
                    }
                }finally {
                    this.lock.unlock();
                }
            } catch (InterruptedException e) {
                log.error("rebalance try lock batch exception", e);
            }
        }
        return lockedMq;
    }


    /**
     * 每个客户端的锁结构
     */
    static class LockEntry{
        private String clientId;
        private volatile long lastUpdateTimestamp = System.currentTimeMillis();

        public boolean isLocked(final String clientId){
            boolean eq = this.clientId.equals(clientId);
            return eq && !this.isExpired();
        }

        public boolean isExpired(){
            return (System.currentTimeMillis() - this.lastUpdateTimestamp) > REBALANCE_LOCK_MAX_IDLE_TIME;
        }

        public String getClientId() {
            return clientId;
        }

        public void setClientId(String clientId) {
            this.clientId = clientId;
        }

        public long getLastUpdateTimestamp() {
            return lastUpdateTimestamp;
        }

        public void setLastUpdateTimestamp(long lastUpdateTimestamp) {
            this.lastUpdateTimestamp = lastUpdateTimestamp;
        }
    }
}
