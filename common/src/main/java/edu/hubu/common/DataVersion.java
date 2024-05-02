package edu.hubu.common;

import edu.hubu.remoting.netty.protocol.RemotingSerialize;

import java.util.concurrent.atomic.AtomicLong;

/**
 * @author: sugar
 * @date: 2023/6/11
 * @description: 用于并发数据版本控制
 */
public class DataVersion extends RemotingSerialize {
    private long timestamp = System.currentTimeMillis();
    private AtomicLong counter = new AtomicLong();

    public void assignNewOne(final DataVersion dataVersion){
        this.timestamp = dataVersion.getTimestamp();
        this.counter.set(dataVersion.getCounter().get());
    }

    public void nextVersion(){
        this.timestamp = System.currentTimeMillis();
        this.counter.incrementAndGet();
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public AtomicLong getCounter() {
        return counter;
    }

    public void setCounter(AtomicLong counter) {
        this.counter = counter;
    }
}
