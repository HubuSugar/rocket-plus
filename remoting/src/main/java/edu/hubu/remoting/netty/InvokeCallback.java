package edu.hubu.remoting.netty;

import edu.hubu.remoting.netty.handler.ResponseFuture;

/**
 * @author: sugar
 * @date: 2023/5/27
 * @description:
 */
public interface InvokeCallback {

    void operationCompleted(final ResponseFuture responseFuture);
}
