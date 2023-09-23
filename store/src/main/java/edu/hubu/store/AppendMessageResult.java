package edu.hubu.store;

/**
 * @author: sugar
 * @date: 2023/7/15
 * @description: 将消息刷盘到commitlog的结果
 */
public class AppendMessageResult {
    private AppendMessageStatus appendMessageStatus;
    //开始写消息的偏移位置
    private long wroteOffset;
    //写消息的大小（长度）
    private int writeBytes;
    //message ID
    private String msgId;
    //消息存储时间
    private long storeTimestamp;
    //消息在consumeQueue的偏移位置
    private long logicOffset;
    //pagecache的响应时间
    private long pagecacheRT;
    //消息数量
    private int msgNum = 1;

    public AppendMessageResult() {
    }

    public AppendMessageResult(AppendMessageStatus appendMessageStatus) {
        this.appendMessageStatus = appendMessageStatus;
    }

    public AppendMessageResult(AppendMessageStatus appendMessageStatus, long wroteOffset, int writeBytes,
                               String msgId, long storeTimestamp, long logicOffset, long pagecacheRT) {
        this.appendMessageStatus = appendMessageStatus;
        this.wroteOffset = wroteOffset;
        this.writeBytes = writeBytes;
        this.msgId = msgId;
        this.storeTimestamp = storeTimestamp;
        this.logicOffset = logicOffset;
        this.pagecacheRT = pagecacheRT;
    }

    public AppendMessageStatus getAppendMessageStatus() {
        return appendMessageStatus;
    }

    public void setAppendMessageStatus(AppendMessageStatus appendMessageStatus) {
        this.appendMessageStatus = appendMessageStatus;
    }

    public long getWroteOffset() {
        return wroteOffset;
    }

    public void setWroteOffset(long wroteOffset) {
        this.wroteOffset = wroteOffset;
    }

    public int getWriteBytes() {
        return writeBytes;
    }

    public void setWriteBytes(int writeBytes) {
        this.writeBytes = writeBytes;
    }

    public String getMsgId() {
        return msgId;
    }

    public void setMsgId(String msgId) {
        this.msgId = msgId;
    }

    public long getStoreTimestamp() {
        return storeTimestamp;
    }

    public void setStoreTimestamp(long storeTimestamp) {
        this.storeTimestamp = storeTimestamp;
    }

    public long getLogicOffset() {
        return logicOffset;
    }

    public void setLogicOffset(long logicOffset) {
        this.logicOffset = logicOffset;
    }

    public long getPagecacheRT() {
        return pagecacheRT;
    }

    public void setPagecacheRT(long pagecacheRT) {
        this.pagecacheRT = pagecacheRT;
    }

    public int getMsgNum() {
        return msgNum;
    }

    public void setMsgNum(int msgNum) {
        this.msgNum = msgNum;
    }
}
