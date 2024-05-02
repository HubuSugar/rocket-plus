package edu.hubu.client.impl.consumer;

import edu.hubu.client.consumer.PullResult;
import edu.hubu.client.consumer.PullStatus;
import edu.hubu.common.message.MessageExt;

import java.util.List;

/**
 * @author: sugar
 * @date: 2023/11/19
 * @description:
 */
public class PullResultExt extends PullResult {
    private final long suggestWhichBrokerId;
    private byte[] messageBinary;

    public PullResultExt(PullStatus pullStatus, long nextBeginOffset, long minOffset, long maxOffset, List<MessageExt> msgFoundList,
                final long suggestWhichBrokerId, final byte[] messageBinary)  {
        super(pullStatus, nextBeginOffset, minOffset, maxOffset, msgFoundList);
        this.suggestWhichBrokerId = suggestWhichBrokerId;
        this.messageBinary = messageBinary;
    }

    public long getSuggestWhichBrokerId() {
        return suggestWhichBrokerId;
    }

    public byte[] getMessageBinary() {
        return messageBinary;
    }

    public void setMessageBinary(byte[] messageBinary) {
        this.messageBinary = messageBinary;
    }
}
