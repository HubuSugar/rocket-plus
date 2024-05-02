package edu.hubu.store;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * @author: sugar
 * @date: 2023/11/26
 * @description:
 */
public class GetMessageResult {
    private final List<SelectMappedBufferResult> msgMappedList = new ArrayList<>(100);
    private final List<ByteBuffer> msgBufferList = new ArrayList<>(100);
    private GetMessageStatus status;
    private long nextBeginOffset;
    private long minOffset;
    private long maxOffset;

    private int bufferTotalSize = 0;
    private boolean suggestPullingFromSlave = false;
    private int msgCount4Commercial = 0;

    public List<SelectMappedBufferResult> getMsgMappedList() {
        return msgMappedList;
    }

    public void addMessage(SelectMappedBufferResult mappedBufferResult) {

    }

    public List<ByteBuffer> getMsgBufferList() {
        return msgBufferList;
    }

    public GetMessageStatus getStatus() {
        return status;
    }

    public void setStatus(GetMessageStatus status) {
        this.status = status;
    }

    public long getNextBeginOffset() {
        return nextBeginOffset;
    }

    public void setNextBeginOffset(long nextBeginOffset) {
        this.nextBeginOffset = nextBeginOffset;
    }

    public long getMinOffset() {
        return minOffset;
    }

    public void setMinOffset(long minOffset) {
        this.minOffset = minOffset;
    }

    public long getMaxOffset() {
        return maxOffset;
    }

    public void setMaxOffset(long maxOffset) {
        this.maxOffset = maxOffset;
    }

    public int getBufferTotalSize() {
        return bufferTotalSize;
    }

    public void setBufferTotalSize(int bufferTotalSize) {
        this.bufferTotalSize = bufferTotalSize;
    }

    public boolean isSuggestPullingFromSlave() {
        return suggestPullingFromSlave;
    }

    public void setSuggestPullingFromSlave(boolean suggestPullingFromSlave) {
        this.suggestPullingFromSlave = suggestPullingFromSlave;
    }

    public int getMsgCount4Commercial() {
        return msgCount4Commercial;
    }

    public void setMsgCount4Commercial(int msgCount4Commercial) {
        this.msgCount4Commercial = msgCount4Commercial;
    }

    public int getMessageCount() {
        return this.msgMappedList.size();
    }

}
