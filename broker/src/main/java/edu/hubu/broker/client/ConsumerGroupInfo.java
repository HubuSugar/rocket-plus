package edu.hubu.broker.client;

import edu.hubu.common.consumer.ConsumeFromWhere;
import edu.hubu.common.protocol.heartbeat.ConsumeType;
import edu.hubu.common.protocol.heartbeat.MessageModel;
import edu.hubu.common.protocol.heartbeat.SubscriptionData;
import io.netty.channel.Channel;
import lombok.extern.slf4j.Slf4j;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * <ul>groupName</ul>
 * <ul>groupName和SubscriptionData对应关系</ul>
 * <ul>channel和channel配置信息</ul>
 *
 * @author: sugar
 * @date: 2023/11/4
 * @description:
 */
@Slf4j
public class ConsumerGroupInfo {
    private final String groupName;
    private final ConcurrentHashMap<String, SubscriptionData> subscriptionTable = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Channel, ClientChannelInfo> channelInfoTable = new ConcurrentHashMap<>(16);

    private volatile ConsumeType consumeType;
    private volatile MessageModel messageModel;
    private volatile ConsumeFromWhere consumeFromWhere;
    private volatile long lastUpdateTimestamp;

    public ConsumerGroupInfo(String groupName, ConsumeType consumeType, MessageModel messageModel, ConsumeFromWhere consumeFromWhere) {
        this.groupName = groupName;
        this.consumeType = consumeType;
        this.messageModel = messageModel;
        this.consumeFromWhere = consumeFromWhere;
    }

    public SubscriptionData findSubscriptionData(String topic) {
        return this.subscriptionTable.get(topic);
    }


    public List<Channel> getAllChannel() {
        return new ArrayList<>(this.channelInfoTable.keySet());
    }

    public List<String> getAllClientId(){
        List<String> ids = new ArrayList<>();
        for (ClientChannelInfo value : channelInfoTable.values()) {
            ids.add(value.getClientId());
        }
        return ids;
    }

    public boolean updateChannel(final ClientChannelInfo newClient, ConsumeType consumeType,
                                 MessageModel messageModel, ConsumeFromWhere consumeFromWhere) {
        boolean updated = false;
        this.consumeType = consumeType;
        this.messageModel = messageModel;
        this.consumeFromWhere = consumeFromWhere;

        ClientChannelInfo oldClient = this.channelInfoTable.get(newClient.getChannel());
        if(oldClient == null){
            ClientChannelInfo prev = this.channelInfoTable.put(newClient.getChannel(), newClient);
            if(prev == null){
                log.info("new client connected, group: {}, {}, {}, channel: {}", groupName, consumeType, messageModel, newClient);
                updated = true;
            }
            oldClient = newClient;
        }else{
            if(!oldClient.getClientId().equals(newClient.getClientId())){
                log.error("[BUG] client exist in broker, but client id not equal, group:{}, old:{}, new:{}", groupName, oldClient, newClient);
                this.channelInfoTable.put(newClient.getChannel(), newClient);
            }
        }

        this.lastUpdateTimestamp = System.currentTimeMillis();
        oldClient.setLastUpdateTimestamp(lastUpdateTimestamp);
        return updated;
    }

    public boolean updateSubscribeData(Set<SubscriptionData> subList) {
        boolean updated = false;

        for (SubscriptionData subscriptionData : subList) {
            SubscriptionData old = this.subscriptionTable.get(subscriptionData.getTopic());
            if(old == null){
                SubscriptionData prev = this.subscriptionTable.putIfAbsent(subscriptionData.getTopic(), subscriptionData);
                if(prev == null){
                  updated = true;
                  log.info("subscription changed, add new topic, group: {}, sub: {} ", groupName, subscriptionData);
                }
            }else if(subscriptionData.getSubVersion() > old.getSubVersion()){
                if(consumeType == ConsumeType.CONSUME_PASSIVELY){
                  log.warn("subscription changed, group: {}, old: {}, new:{}", groupName, old, subscriptionData);
                }
                this.subscriptionTable.put(subscriptionData.getTopic(), subscriptionData);
            }
        }

        Iterator<Map.Entry<String, SubscriptionData>> iterator = this.subscriptionTable.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, SubscriptionData> next = iterator.next();
            String oldTopic = next.getKey();

            boolean exist = false;
            for (SubscriptionData data : subList) {
                if (data.getTopic().equals(oldTopic)) {
                    exist = true;
                    break;
                }
            }

            if(!exist){
                log.warn("subscription changed, group: {} remove topic {} {}",
                        this.groupName,
                        oldTopic,
                        next.getValue()
                );
                iterator.remove();
                updated = true;
            }
        }

        this.lastUpdateTimestamp = System.currentTimeMillis();
        return updated;
    }


    public String getGroupName() {
        return groupName;
    }

    public ConcurrentHashMap<String, SubscriptionData> getSubscriptionTable() {
        return subscriptionTable;
    }

    public ConcurrentHashMap<Channel, ClientChannelInfo> getChannelInfoTable() {
        return channelInfoTable;
    }

    public ConsumeType getConsumeType() {
        return consumeType;
    }

    public MessageModel getMessageModel() {
        return messageModel;
    }

    public ConsumeFromWhere getConsumeFromWhere() {
        return consumeFromWhere;
    }

    public long getLastUpdateTimestamp() {
        return lastUpdateTimestamp;
    }

    public void setLastUpdateTimestamp(long lastUpdateTimestamp) {
        this.lastUpdateTimestamp = lastUpdateTimestamp;
    }

}
