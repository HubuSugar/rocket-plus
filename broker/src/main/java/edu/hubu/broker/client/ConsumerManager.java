package edu.hubu.broker.client;

import edu.hubu.common.consumer.ConsumeFromWhere;
import edu.hubu.common.protocol.heartbeat.ConsumeType;
import edu.hubu.common.protocol.heartbeat.MessageModel;
import edu.hubu.common.protocol.heartbeat.SubscriptionData;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 消费者组的对应关系
 * @author: sugar
 * @date: 2023/11/4
 * @description: 管理消费者和消费关系
 */
public class ConsumerManager{

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
}
