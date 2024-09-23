package edu.hubu.namesrv.routeInfo;

import edu.hubu.namesrv.NamesrvController;
import edu.hubu.remoting.netty.ChannelEventListener;
import io.netty.channel.Channel;

/**
 * @author: sugar
 * @date: 2024/9/18
 * @description:
 */
public class BrokerHousekeepingService implements ChannelEventListener {

    private final NamesrvController namesrvController;

    public BrokerHousekeepingService(NamesrvController namesrvController) {
        this.namesrvController = namesrvController;
    }

    @Override
    public void onChannelConnect(String remoteAddress, Channel channel) {

    }

    @Override
    public void onChannelClose(String remoteAddress, Channel channel) {
        this.namesrvController.getTopicRouteInfoManager().onChannelDestroy(remoteAddress, channel);
    }

    @Override
    public void onChannelException(String remoteAddress, Channel channel) {
        this.namesrvController.getTopicRouteInfoManager().onChannelDestroy(remoteAddress, channel);
    }

    @Override
    public void onChannelIdle(String remoteAddress, Channel channel) {
        this.namesrvController.getTopicRouteInfoManager().onChannelDestroy(remoteAddress, channel);
    }
}
