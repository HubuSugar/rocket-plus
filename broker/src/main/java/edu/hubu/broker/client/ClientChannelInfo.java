package edu.hubu.broker.client;

import io.netty.channel.Channel;

/**
 * @author: sugar
 * @date: 2023/11/4
 * @description:
 */
public class ClientChannelInfo {
    private final Channel channel;
    private final String clientId;

    public ClientChannelInfo(Channel channel, String clientId) {
        this.channel = channel;
        this.clientId = clientId;
    }

    public Channel getChannel() {
        return channel;
    }

    public String getClientId() {
        return clientId;
    }
}
