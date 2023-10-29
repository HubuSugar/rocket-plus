package edu.hubu.store;

import edu.hubu.common.message.Message;
import edu.hubu.common.message.MessageExtBrokerInner;

import java.util.concurrent.CompletableFuture;

/**
 * @author: sugar
 * @date: 2023/7/15
 * @description:
 */
public interface MessageStore {

    boolean load();

    void start() throws Exception;

    default CompletableFuture<PutMessageResult> asyncPutMessage(final MessageExtBrokerInner message){
        return CompletableFuture.completedFuture(putMessage(message));
    }

    PutMessageResult putMessage(final MessageExtBrokerInner message);

    void cleanExpiredConsumeQueue();
}
