package edu.hubu.store;

import java.nio.MappedByteBuffer;

/**
 * @author: sugar
 * @date: 2023/8/7
 * @description: 记录操作位点
 */
public class StoreCheckpoint {

    // private final MappedByteBuffer mappedByteBuffer;
    private volatile long physicalMsgTimestamp = 0;
    private volatile long logicMsgTimestamp = 0;

    public StoreCheckpoint() {
        // this.mappedByteBuffer = mappedByteBuffer;
    }

    public void flush(){
    }


    public long getPhysicalMsgTimestamp() {
        return physicalMsgTimestamp;
    }

    public void setPhysicalMsgTimestamp(long physicalMsgTimestamp) {
        this.physicalMsgTimestamp = physicalMsgTimestamp;
    }

    public long getLogicMsgTimestamp() {
        return logicMsgTimestamp;
    }

    public void setLogicMsgTimestamp(long logicMsgTimestamp) {
        this.logicMsgTimestamp = logicMsgTimestamp;
    }
}
