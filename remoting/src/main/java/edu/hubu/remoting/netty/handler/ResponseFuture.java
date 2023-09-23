package edu.hubu.remoting.netty.handler;

import edu.hubu.remoting.netty.InvokeCallback;
import edu.hubu.remoting.netty.RemotingCommand;
import io.netty.channel.Channel;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

/**
 * @author: sugar
 * @date: 2023/5/19
 * @description:
 */
public class ResponseFuture {
    private final int opaque;
    private final Channel processChannel;
    private final InvokeCallback callback;
    private final long timeoutMillis;
    private final CountDownLatch countDownLatch = new CountDownLatch(1);

    // private final SemaphoreReleaseOnlyOnce once;

    private volatile RemotingCommand responseCommand;
    private volatile boolean sendRequestOk;
    private volatile Throwable cause;

    public ResponseFuture(int opaque, Channel processChannel, InvokeCallback callback, long timeoutMillis) {
        this.opaque = opaque;
        this.processChannel = processChannel;
        this.callback = callback;
        this.timeoutMillis = timeoutMillis;
    }

    public RemotingCommand waitResponse(final long timeoutMillis) throws InterruptedException {
        this.countDownLatch.await(timeoutMillis, TimeUnit.MILLISECONDS);
        return this.responseCommand;
    }

    public void putResponse(RemotingCommand responseCommand){
        this.responseCommand = responseCommand;
        countDownLatch.countDown();
    }

    public void release(){
        // if(once != null){
        //     this.once.release();
        // }
    }

    public int getOpaque() {
        return opaque;
    }

    public Channel getProcessChannel() {
        return processChannel;
    }

    public InvokeCallback getCallback() {
        return callback;
    }

    public long getTimeoutMillis() {
        return timeoutMillis;
    }

    public CountDownLatch getCountDownLatch() {
        return countDownLatch;
    }

    public RemotingCommand getResponseCommand() {
        return responseCommand;
    }

    public void setResponseCommand(RemotingCommand responseCommand) {
        this.responseCommand = responseCommand;
    }

    public boolean isSendRequestOk() {
        return sendRequestOk;
    }

    public void setSendRequestOk(boolean sendRequestOk) {
        this.sendRequestOk = sendRequestOk;
    }

    public Throwable getCause() {
        return cause;
    }

    public void setCause(Throwable cause) {
        this.cause = cause;
    }
}
