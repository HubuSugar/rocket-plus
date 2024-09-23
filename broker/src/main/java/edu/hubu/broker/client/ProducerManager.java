package edu.hubu.broker.client;

import edu.hubu.remoting.netty.common.RemotingHelper;
import edu.hubu.remoting.netty.common.RemotingUtil;
import io.netty.channel.Channel;
import lombok.extern.slf4j.Slf4j;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author: sugar
 * @date: 2024/7/27
 * @description:
 */
@Slf4j
public class ProducerManager {

    private static final long CHANNEL_EXPIRED_TIMEOUT = 1000 * 120;

    //<groupName, map>
    private final ConcurrentHashMap<String, ConcurrentHashMap<Channel, ClientChannelInfo>> groupChannelTable = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Channel> clientChannelTable = new ConcurrentHashMap<>();

    public synchronized void registerProducer(final String group, final ClientChannelInfo clientChannelInfo) {
        ClientChannelInfo clientChannelInfoFound = null;
        ConcurrentHashMap<Channel, ClientChannelInfo> channelTable = this.groupChannelTable.get(group);
        if(channelTable == null){
            channelTable = new ConcurrentHashMap<>();
            this.groupChannelTable.put(group, channelTable);
        }

        clientChannelInfoFound = channelTable.get(clientChannelInfo.getChannel());
        if(clientChannelInfoFound == null){
            channelTable.put(clientChannelInfo.getChannel(), clientChannelInfo);
            clientChannelTable.put(clientChannelInfo.getClientId(), clientChannelInfo.getChannel());
            log.info("new producer connected, group:{}, channel:{}", group, clientChannelInfo);
        }

        if(clientChannelInfoFound != null){
            clientChannelInfoFound.setLastUpdateTimestamp(System.currentTimeMillis());
        }
    }

    public void scanNotActiveChannel() {
        for (final Map.Entry<String, ConcurrentHashMap<Channel, ClientChannelInfo>> entry : this.groupChannelTable.entrySet()) {
            final String group = entry.getKey();
            final ConcurrentHashMap<Channel, ClientChannelInfo> channelTable = entry.getValue();

            Iterator<Map.Entry<Channel, ClientChannelInfo>> iterator = channelTable.entrySet().iterator();
            while (iterator.hasNext()){
                Map.Entry<Channel, ClientChannelInfo> item = iterator.next();
                final ClientChannelInfo channelInfo = item.getValue();

                long diff = System.currentTimeMillis() - channelInfo.getLastUpdateTimestamp();
                if(diff > CHANNEL_EXPIRED_TIMEOUT){
                    iterator.remove();
                    this.clientChannelTable.remove(channelInfo.getClientId());
                    log.warn("SCAN:remove expired channel:[{}] from producer manager groupChannelTable, producer group name:{}", RemotingHelper.parseChannel2RemoteAddress(channelInfo.getChannel()), group);
                    RemotingUtil.closeChannel(channelInfo.getChannel());
                }
            }

        }
    }

    public synchronized void doChannelCloseEvent(String remoteAddress, Channel channel) {
        if(channel != null){
            for (Map.Entry<String, ConcurrentHashMap<Channel, ClientChannelInfo>> entry : this.groupChannelTable.entrySet()) {
                final String group = entry.getKey();
                final ConcurrentHashMap<Channel, ClientChannelInfo> channelTable = entry.getValue();
                final ClientChannelInfo clientChannelInfo = channelTable.remove(channel);

                if(clientChannelInfo != null){
                    this.clientChannelTable.remove(group);
                    log.info("NETTY EVENT: remove channel [{}][{}] from ProducerManager groupChannelTable, producer group: {}", clientChannelInfo.toString(), remoteAddress, group);
                }
            }
        }
    }
}
