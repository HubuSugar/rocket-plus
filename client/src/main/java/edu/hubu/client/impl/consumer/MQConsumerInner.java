package edu.hubu.client.impl.consumer;

import edu.hubu.common.consumer.ConsumeFromWhere;
import edu.hubu.common.message.MessageQueue;
import edu.hubu.common.protocol.heartbeat.ConsumeType;
import edu.hubu.common.protocol.heartbeat.MessageModel;
import edu.hubu.common.protocol.heartbeat.SubscriptionData;

import java.util.Set;

/**
 * @author: sugar
 * @date: 2023/10/28
 * @description:
 */
public interface MQConsumerInner {

    String groupName();

    MessageModel messageModel();

    ConsumeType consumeType();

    ConsumeFromWhere consumeFromWhere();

    Set<SubscriptionData> subscriptions();

    void doRebalance();

    boolean isUnitMode();

    void updateTopicSubscribeInfo(String topic, Set<MessageQueue> topicSubscribeInfo);

    boolean isNeedUpdateTopicRoute(String topic);
}
