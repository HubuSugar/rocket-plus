package edu.hubu.store.consumeQueue;

import java.util.Map;

/**
 * @author: sugar
 * @date: 2023/8/19
 * @description:
 */
public class DispatchRequest {

    private final String topic;
    private final int queueId;
    private final long commitLogOffset;
    private int msgSize;
    private final long tagsCode;
    private final long storeTimestamp;
    private final long consumeQueueOffset;
    private final String keys;
    private final boolean success;
    private final String uniqKey;

    private final int sysFlag;
    private final long prepareTransactionOffset;
    private final Map<String, String> propertiesMap;
    private byte[] bitMap;

    private int bufferSize = -1;

    public DispatchRequest(String topic, int queueId, long commitLogOffset, int msgSize, long tagsCode,
                           long storeTimestamp, long consumeQueueOffset, String keys, String uniqKey,
                           int sysFlag, long prepareTransactionOffset, Map<String, String> propertiesMap) {
        this.topic = topic;
        this.queueId = queueId;
        this.commitLogOffset = commitLogOffset;
        this.msgSize = msgSize;
        this.tagsCode = tagsCode;
        this.storeTimestamp = storeTimestamp;
        this.consumeQueueOffset = consumeQueueOffset;
        this.keys = keys;
        this.success = true;
        this.uniqKey = uniqKey;
        this.sysFlag = sysFlag;
        this.prepareTransactionOffset = prepareTransactionOffset;
        this.propertiesMap = propertiesMap;
    }

    public DispatchRequest(int msgSize) {
        this.topic = "";
        this.queueId = 0;
        this.commitLogOffset = 0;
        this.msgSize = msgSize;
        this.tagsCode = 0;
        this.storeTimestamp = 0;
        this.consumeQueueOffset = 0;
        this.keys = "";
        this.uniqKey = null;
        this.sysFlag = 0;
        this.prepareTransactionOffset = 0;
        this.success = false;
        this.propertiesMap = null;
    }

    public DispatchRequest(int msgSize, boolean success) {
        this.topic = "";
        this.queueId = 0;
        this.commitLogOffset = 0;
        this.msgSize = msgSize;
        this.tagsCode = 0;
        this.storeTimestamp = 0;
        this.consumeQueueOffset = 0;
        this.keys = "";
        this.uniqKey = null;
        this.sysFlag = 0;
        this.prepareTransactionOffset = 0;
        this.success = success;
        this.propertiesMap = null;
    }

    public String getTopic() {
        return topic;
    }

    public int getQueueId() {
        return queueId;
    }

    public long getCommitLogOffset() {
        return commitLogOffset;
    }

    public int getMsgSize() {
        return msgSize;
    }

    public void setMsgSize(int msgSize) {
        this.msgSize = msgSize;
    }

    public long getTagsCode() {
        return tagsCode;
    }

    public long getStoreTimestamp() {
        return storeTimestamp;
    }

    public long getConsumeQueueOffset() {
        return consumeQueueOffset;
    }

    public String getKeys() {
        return keys;
    }

    public boolean isSuccess() {
        return success;
    }

    public String getUniqKey() {
        return uniqKey;
    }

    public int getSysFlag() {
        return sysFlag;
    }

    public long getPrepareTransactionOffset() {
        return prepareTransactionOffset;
    }

    public Map<String, String> getPropertiesMap() {
        return propertiesMap;
    }

    public byte[] getBitMap() {
        return bitMap;
    }

    public void setBitMap(byte[] bitMap) {
        this.bitMap = bitMap;
    }

    public int getBufferSize() {
        return bufferSize;
    }

    public void setBufferSize(int bufferSize) {
        this.bufferSize = bufferSize;
    }
}
