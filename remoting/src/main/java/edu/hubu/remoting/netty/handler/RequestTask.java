package edu.hubu.remoting.netty.handler;

import edu.hubu.remoting.netty.RemotingCommand;
import io.netty.channel.Channel;


/**
 * @author: sugar
 * @date: 2023/5/21
 * @description:
 */
public class RequestTask implements Runnable {
    private final Runnable r;
    private final long createTimestamp = System.currentTimeMillis();
    private final Channel channel;
    private final RemotingCommand request;
    private boolean stopRun = false;

    public RequestTask(final Runnable r, final Channel channel, final RemotingCommand request) {
        this.r = r;
        this.channel = channel;
        this.request = request;
    }

    @Override
    public void run() {
        if(!stopRun){
            this.r.run();
        }
    }

    @Override
    public int hashCode() {
        int result = r != null ? r.hashCode() : 0;
        result = 31 * result + (int) (getCreateTimestamp() ^ (getCreateTimestamp() >>> 32));
        result = 31 * result + (channel != null ? channel.hashCode() : 0);
        result = 31 * result + (request != null ? request.hashCode() : 0);
        result = 31 * result + (isStopRun() ? 1 : 0);
        return result;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o)
            return true;
        if (!(o instanceof RequestTask))
            return false;

        final RequestTask that = (RequestTask) o;

        if (getCreateTimestamp() != that.getCreateTimestamp())
            return false;
        if (isStopRun() != that.isStopRun())
            return false;
        if (channel != null ? !channel.equals(that.channel) : that.channel != null)
            return false;
        return request != null ? request.getOpaque() == that.request.getOpaque() : that.request == null;

    }

    public Channel getChannel() {
        return channel;
    }

    public RemotingCommand getRequest() {
        return request;
    }

    public long getCreateTimestamp() {
        return createTimestamp;
    }

    public boolean isStopRun() {
        return stopRun;
    }

    public void setStopRun(boolean stopRun) {
        this.stopRun = stopRun;
    }
}
