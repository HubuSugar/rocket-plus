package edu.hubu.client.producer;

import edu.hubu.client.impl.TopicPublishInfo;

import java.util.Set;

/**
 * @author: sugar
 * @date: 2023/6/3
 * @description:
 */
public interface MQProducerInner {

    boolean isNeedUpdateTopicRoute(String topic);

    void updateTopicPublishInfo(String topic, TopicPublishInfo publishInfo);

    Set<String> getPublishTopicList();
}
