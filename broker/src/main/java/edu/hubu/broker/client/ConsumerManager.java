package edu.hubu.broker.client;

import edu.hubu.common.consumer.ConsumeFromWhere;
import edu.hubu.common.protocol.heartbeat.ConsumeType;
import edu.hubu.common.protocol.heartbeat.MessageModel;
import edu.hubu.common.protocol.heartbeat.SubscriptionData;
import edu.hubu.remoting.netty.common.RemotingHelper;
import edu.hubu.remoting.netty.common.RemotingUtil;
import io.netty.channel.Channel;
import lombok.extern.slf4j.Slf4j;

import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 消费者组的对应关系
 * @author: sugar
 * @date: 2023/11/4
 * @description: 管理消费者和消费关系
 */
@Slf4j
public class ConsumerManager{

    private static final long CHANNEL_EXPIRED_TIMEOUT = 120 * 1000;
    private final ConcurrentHashMap<String, ConsumerGroupInfo> consumerTable = new ConcurrentHashMap<>(1024);

    private final ConsumerIdsChangeListener consumerIdsChangeListener;

    public ConsumerManager(ConsumerIdsChangeListener consumerIdsChangeListener) {
        this.consumerIdsChangeListener = consumerIdsChangeListener;
    }

    public ConsumerGroupInfo getConsumerGroupInfo(String consumerGroup) {
        return this.consumerTable.get(consumerGroup);
    }

    public ConcurrentHashMap<String, ConsumerGroupInfo> getConsumerTable() {
        return consumerTable;
    }

    public boolean registerConsumer(final String groupName,final ClientChannelInfo clientChannelInfo, ConsumeType consumerType,
                                    MessageModel messageModel, ConsumeFromWhere consumeFromWhere, final Set<SubscriptionData> subList,
                                    boolean isNotifyConsumerIdsChangeEnable) {
        ConsumerGroupInfo consumerGroupInfo = this.consumerTable.get(groupName);
        if(consumerGroupInfo == null){
            ConsumerGroupInfo tmp = new ConsumerGroupInfo(groupName, consumerType, messageModel, consumeFromWhere);
            ConsumerGroupInfo prev = this.consumerTable.putIfAbsent(groupName, tmp);
            consumerGroupInfo = prev != null ? prev : tmp;
        }

        boolean r1 = consumerGroupInfo.updateChannel(clientChannelInfo, consumerType, messageModel, consumeFromWhere);
        boolean r2 = consumerGroupInfo.updateSubscribeData(subList);

        if(r1 || r2){
            if(isNotifyConsumerIdsChangeEnable){
                 this.consumerIdsChangeListener.handle(ConsumerGroupEvent.CHANGED, groupName, consumerGroupInfo.getAllChannel());
            }
        }

        this.consumerIdsChangeListener.handle(ConsumerGroupEvent.REGISTER, groupName, subList);

        return r1 || r2;
    }

    public void scanNotActiveChannel() {
        Iterator<Entry<String, ConsumerGroupInfo>> it = this.consumerTable.entrySet().iterator();
        while (it.hasNext()){
            Entry<String, ConsumerGroupInfo> next = it.next();
            String consumerGroup = next.getKey();
            ConsumerGroupInfo consumerGroupInfo = next.getValue();
            ConcurrentHashMap<Channel, ClientChannelInfo> channelInfoTable = consumerGroupInfo.getChannelInfoTable();
            Iterator<Entry<Channel, ClientChannelInfo>> iterator = channelInfoTable.entrySet().iterator();
            while (iterator.hasNext()){
                Entry<Channel, ClientChannelInfo> entry = iterator.next();
                ClientChannelInfo clientChannelInfo = entry.getValue();
                long diff = System.currentTimeMillis() - clientChannelInfo.getLastUpdateTimestamp();
                if(diff > CHANNEL_EXPIRED_TIMEOUT){
                    log.warn("Scan: remove expired channel from consumer manager consumerTable, channel={}, consumerGroup = {}",
                            RemotingHelper.parseChannel2RemoteAddress(clientChannelInfo.getChannel()),
                            consumerGroup);
                    RemotingUtil.closeChannel(clientChannelInfo.getChannel());
                    iterator.remove();
                }
            }

            if(channelInfoTable.isEmpty()){
                log.warn("Scan: remove expred channel from consumer manager consumerTable, all clear, consumerGroup: {}", consumerGroup);
               it.remove();
            }
        }
    }

    public void doChannelCloseEvent(String remoteAddress, Channel channel) {
        for (Entry<String, ConsumerGroupInfo> entry : this.consumerTable.entrySet()) {
            ConsumerGroupInfo consumerGroupInfo = entry.getValue();
            boolean removed = consumerGroupInfo.doChannelCloseEvent(remoteAddress, channel);
            if (removed) {
                if (consumerGroupInfo.getChannelInfoTable().isEmpty()) {
                    ConsumerGroupInfo remove = this.consumerTable.remove(entry.getKey());
                    if (remove != null) {
                        log.info("unregister consumer ok, no any connection and remove consumer group, {}", entry.getKey());
                        this.consumerIdsChangeListener.handle(ConsumerGroupEvent.UNREGISTER, entry.getKey());
                    }
                }

                this.consumerIdsChangeListener.handle(ConsumerGroupEvent.UNREGISTER, entry.getKey());
            }
        }
    }
}
