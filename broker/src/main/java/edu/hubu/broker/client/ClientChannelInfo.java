package edu.hubu.broker.client;

import edu.hubu.remoting.netty.protocol.LanguageCode;
import io.netty.channel.Channel;
import lombok.EqualsAndHashCode;

/**
 * @author: sugar
 * @date: 2023/11/4
 * @description:
 */
@EqualsAndHashCode
public class ClientChannelInfo {
    private final Channel channel;
    private final String clientId;
    private final LanguageCode languageCode;
    private final int version;
    private volatile long lastUpdateTimestamp = System.currentTimeMillis();

    public ClientChannelInfo(Channel channel) {
        this(channel, null, null, 0);
    }

    public ClientChannelInfo(Channel channel, String clientId, LanguageCode languageCode, int version) {
        this.channel = channel;
        this.clientId = clientId;
        this.languageCode = languageCode;
        this.version = version;
    }

    public Channel getChannel() {
        return channel;
    }

    public String getClientId() {
        return clientId;
    }

    public long getLastUpdateTimestamp() {
        return lastUpdateTimestamp;
    }

    public void setLastUpdateTimestamp(long lastUpdateTimestamp) {
        this.lastUpdateTimestamp = lastUpdateTimestamp;
    }
}
