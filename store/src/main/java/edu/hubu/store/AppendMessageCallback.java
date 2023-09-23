package edu.hubu.store;

import edu.hubu.common.message.MessageExt;
import edu.hubu.common.message.MessageExtBatch;
import edu.hubu.common.message.MessageExtBrokerInner;

import java.nio.ByteBuffer;

/**
 * @author: sugar
 * @date: 2023/7/16
 * @description:
 */
public interface AppendMessageCallback {

   AppendMessageResult doAppend(long fileFromOffset, ByteBuffer byteBuffer, int maxBlank,  MessageExtBrokerInner messageInner);

   AppendMessageResult doAppend(long fileFromOffset, ByteBuffer byteBuffer, int maxBlank, MessageExtBatch messageExtBatch);
}
