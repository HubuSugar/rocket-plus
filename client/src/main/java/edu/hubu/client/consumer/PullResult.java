package edu.hubu.client.consumer;

import edu.hubu.common.message.MessageExt;

import java.util.List;

/**
 * @author: sugar
 * @date: 2023/11/7
 * @description:
 */
public class PullResult {
    private final PullStatus pullStatus;
    private final long nextBeginOffset;
    private final long minOffset;
    private final long maxOffset;
    private final List<MessageExt> msgFoundList;

    public PullResult(PullStatus pullStatus, long nextBeginOffset, long minOffset, long maxOffset, List<MessageExt> msgFoundList) {
        this.pullStatus = pullStatus;
        this.nextBeginOffset = nextBeginOffset;
        this.minOffset = minOffset;
        this.maxOffset = maxOffset;
        this.msgFoundList = msgFoundList;
    }

    public PullStatus getPullStatus() {
        return pullStatus;
    }

    public long getNextBeginOffset() {
        return nextBeginOffset;
    }

    public long getMinOffset() {
        return minOffset;
    }

    public long getMaxOffset() {
        return maxOffset;
    }

    public List<MessageExt> getMsgFoundList() {
        return msgFoundList;
    }
}
