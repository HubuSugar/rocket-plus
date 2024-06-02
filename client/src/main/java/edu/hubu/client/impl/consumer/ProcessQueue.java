package edu.hubu.client.impl.consumer;

import edu.hubu.common.message.MessageConst;
import edu.hubu.common.message.MessageExt;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * @author: sugar
 * @date: 2023/10/29
 * @description: 消息消费者的本地缓存
 */
@Slf4j
public class ProcessQueue {

    private static final long PULL_MAX_IDLE_TIME = Long.parseLong(System.getProperty("rocketmq.client.pull.pullMaxIdleTime", "1200000"));
    //本地消息读写锁
    private final ReadWriteLock lockTreeMap = new ReentrantReadWriteLock();
    private final TreeMap<Long, MessageExt> msgTreeMap = new TreeMap<>();
    //缓存的消息大小
    private final AtomicLong msgCount = new AtomicLong();
    private final AtomicLong msgSize = new AtomicLong();

    //当前队列中最大偏移量
    private volatile long queueOffsetMax = 0L;
    private volatile boolean dropped = false;
    private volatile long lastPullTimestamp = System.currentTimeMillis();
    private volatile long lastConsumeTimestamp = System.currentTimeMillis();

    private volatile boolean locked = false;
    private volatile long lastLockTimestamp= System.currentTimeMillis();
    private volatile boolean consuming = false;
    private volatile long msgAccCnt = 0;


    public boolean isPullExpired(){
        return (System.currentTimeMillis() - this.lastPullTimestamp) > PULL_MAX_IDLE_TIME;
    }

    public boolean putMessage(List<MessageExt> msgs) {
        boolean dispatchToConsume = false;
        try{
            this.lockTreeMap.writeLock().lockInterruptibly();
            try{
                int validMsgCnt = 0;
                for (MessageExt msg : msgs) {
                    MessageExt old = msgTreeMap.put(msg.getQueueOffset(), msg);
                    if(old == null){
                        validMsgCnt++;
                        this.queueOffsetMax = msg.getQueueOffset();
                        msgSize.addAndGet(msg.getBody().length);
                    }
                }

                msgCount.addAndGet(validMsgCnt);

                if(!msgTreeMap.isEmpty() && !this.consuming){
                    dispatchToConsume = true;
                    this.consuming = true;
                }

                if(!msgs.isEmpty()){
                    MessageExt messageExt = msgs.get(msgs.size() - 1);
                    String property = messageExt.getProperty(MessageConst.PROPERTY_MAX_OFFSET);
                    if(property != null){
                        long msgAccCnt = Long.parseLong(property) - messageExt.getQueueOffset();
                        if(msgAccCnt > 0){
                            this.msgAccCnt = msgAccCnt;
                        }
                    }

                }

            }finally {
                this.lockTreeMap.writeLock().unlock();
            }
        } catch (InterruptedException e) {
            log.error("put message lock exception", e);
        }
        return dispatchToConsume;
    }

    public long getMaxSpan() {
        try {
            this.lockTreeMap.readLock().lockInterruptibly();
            try{
                if(!this.msgTreeMap.isEmpty()){
                  return this.msgTreeMap.lastKey() - this.msgTreeMap.firstKey();
                }
            }finally {
                this.lockTreeMap.readLock().unlock();
            }
        } catch (InterruptedException e) {
            log.error("lock msg tree exception", e);
        }
        return 0;
    }

    public long removeMessage(List<MessageExt> msgs) {
        long result = -1;
        try{
            long now = System.currentTimeMillis();
            this.lockTreeMap.writeLock().lockInterruptibly();
            this.lastConsumeTimestamp = now;
            try{
                if(!this.msgTreeMap.isEmpty()){
                    result = this.queueOffsetMax + 1;
                    int removeCnt = 0;
                    for (MessageExt msg : msgs) {
                        MessageExt prev = this.msgTreeMap.remove(msg.getQueueOffset());
                        if(prev != null){
                            removeCnt--;
                            //更新消息的总大小
                            msgSize.addAndGet(0 - msg.getBody().length);
                        }
                    }
                    //更新消息的总条数
                    msgCount.addAndGet(removeCnt);

                    if(!this.msgTreeMap.isEmpty()){
                        result = msgTreeMap.firstKey();
                    }
                }
            }finally {
                this.lockTreeMap.writeLock().unlock();
            }
        }catch (Throwable t){
            log.error("remove message exception", t);
        }
        return result;
    }

    public TreeMap<Long, MessageExt> getMsgTreeMap() {
        return msgTreeMap;
    }

    public AtomicLong getMsgCount() {
        return msgCount;
    }

    public AtomicLong getMsgSize() {
        return msgSize;
    }

    public boolean isDropped() {
        return dropped;
    }

    public void setDropped(boolean dropped) {
        this.dropped = dropped;
    }

    public long getLastPullTimestamp() {
        return lastPullTimestamp;
    }

    public void setLastPullTimestamp(long lastPullTimestamp) {
        this.lastPullTimestamp = lastPullTimestamp;
    }

    public boolean isLocked() {
        return locked;
    }

    public void setLocked(boolean locked) {
        this.locked = locked;
    }

    public long getLastLockTimestamp() {
        return lastLockTimestamp;
    }

    public void setLastLockTimestamp(long lastLockedTimestamp) {
        this.lastLockTimestamp = lastLockedTimestamp;
    }

}
