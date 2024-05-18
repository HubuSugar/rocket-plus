package edu.hubu.broker.longpolling;

import edu.hubu.common.protocol.heartbeat.SubscriptionData;
import edu.hubu.remoting.netty.RemotingCommand;
import edu.hubu.store.MessageFilter;
import io.netty.channel.Channel;


/**
 * @author: sugar
 * @date: 2024/5/4
 * @description:
 */
public class PullRequest {
    private final RemotingCommand remotingCommand;
    private final Channel channel;
    private final long timeoutMillis;
    private final long pullFromThisOffset;
    private final long suspendTimeMillis;
    private final SubscriptionData subscriptionData;
    private final MessageFilter messageFilter;

    public PullRequest(RemotingCommand remotingCommand, Channel channel, long timeoutMillis,
                       long pullFromThisOffset, long suspendTimeMillis, SubscriptionData subscriptionData, MessageFilter messageFilter) {
        this.remotingCommand = remotingCommand;
        this.channel = channel;
        this.timeoutMillis = timeoutMillis;
        this.pullFromThisOffset = pullFromThisOffset;
        this.suspendTimeMillis = suspendTimeMillis;
        this.subscriptionData = subscriptionData;
        this.messageFilter = messageFilter;
    }

    public RemotingCommand getRemotingCommand() {
        return remotingCommand;
    }

    public Channel getChannel() {
        return channel;
    }

    public long getTimeoutMillis() {
        return timeoutMillis;
    }

    public long getPullFromThisOffset() {
        return pullFromThisOffset;
    }

    public long getSuspendTimeMillis() {
        return suspendTimeMillis;
    }

    public SubscriptionData getSubscriptionData() {
        return subscriptionData;
    }

    public MessageFilter getMessageFilter() {
        return messageFilter;
    }
}
