package edu.hubu.broker.client;

import edu.hubu.common.consumer.ConsumeFromWhere;
import edu.hubu.common.protocol.heartbeat.ConsumeType;
import edu.hubu.common.protocol.heartbeat.MessageModel;
import edu.hubu.common.protocol.heartbeat.SubscriptionData;
import io.netty.channel.Channel;

import java.util.ArrayList;
import java.util.List;
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
public class ConsumerGroupInfo {
    private final String groupName;
    private final ConcurrentHashMap<String, SubscriptionData> subscriptionTable = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Channel, ClientChannelInfo> channelInfoTable = new ConcurrentHashMap<>(16);

    private final ConsumeType consumeType;
    private final MessageModel messageModel;
    private final ConsumeFromWhere consumeFromWhere;

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


    public List<String> getAllClientId(){
        List<String> ids = new ArrayList<>();
        for (ClientChannelInfo value : channelInfoTable.values()) {
            ids.add(value.getClientId());
        }
        return ids;
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
