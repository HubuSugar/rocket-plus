package edu.hubu.remoting.netty.handler;

import io.netty.channel.Channel;

/**
 * @author: sugar
 * @date: 2024/8/18
 * @description:
 */
public class NettyEvent {
    private final NettyEventType type;
    private final String remoteAddr;
    private final Channel channel;

    public NettyEvent(NettyEventType nettyEventType, String remoteAddr, Channel channel) {
        this.type = nettyEventType;
        this.remoteAddr = remoteAddr;
        this.channel = channel;
    }

    public NettyEventType getType() {
        return type;
    }

    public String getRemoteAddr() {
        return remoteAddr;
    }

    public Channel getChannel() {
        return channel;
    }
}
