package edu.hubu.remoting.netty.handler;

import edu.hubu.remoting.netty.RemotingCommand;
import io.netty.channel.Channel;


/**
 * @author: sugar
 * @date: 2023/5/21
 * @description:
 */
public class RequestTask implements Runnable{
    private Runnable r;
    private Channel channel;
    private RemotingCommand request;

    public RequestTask(Runnable r, Channel channel, RemotingCommand request) {
        this.r = r;
        this.channel = channel;
        this.request = request;
    }

    @Override
    public void run() {
        this.r.run();
    }

    public Channel getChannel() {
        return channel;
    }

    public void setChannel(Channel channel) {
        this.channel = channel;
    }

    public RemotingCommand getRequest() {
        return request;
    }

    public void setRequest(RemotingCommand request) {
        this.request = request;
    }
}
