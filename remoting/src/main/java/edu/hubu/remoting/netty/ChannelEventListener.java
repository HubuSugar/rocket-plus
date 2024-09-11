package edu.hubu.remoting.netty;

import io.netty.channel.Channel;

/**
 * @author: sugar
 * @date: 2023/5/16
 * @description:
 */
public interface ChannelEventListener {

    void onChannelConnect(final String remoteAddress, final Channel channel);

    void onChannelClose(final String remoteAddress, final Channel channel);

    void onChannelException(final String remoteAddress, final Channel channel);

    void onChannelIdle(final String remoteAddress, final Channel channel);

}
