package edu.hubu.broker.client;

import java.util.concurrent.ConcurrentHashMap;

/**
 * 消费者组的对应关系
 * @author: sugar
 * @date: 2023/11/4
 * @description: 管理消费者和消费关系
 */
public class ConsumerManager{

    private final ConcurrentHashMap<String, ConsumerGroupInfo> consumerTable = new ConcurrentHashMap<>(1024);

    public ConsumerGroupInfo getConsumerGroupInfo(String consumerGroup) {
        return this.consumerTable.get(consumerGroup);
    }

    public ConcurrentHashMap<String, ConsumerGroupInfo> getConsumerTable() {
        return consumerTable;
    }
}
