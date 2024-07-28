package edu.hubu.broker.client;

import io.netty.channel.Channel;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.ConcurrentHashMap;

/**
 * @author: sugar
 * @date: 2024/7/27
 * @description:
 */
@Slf4j
public class ProducerManager {

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
}
