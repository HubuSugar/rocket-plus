package edu.hubu.broker.client;

import java.util.concurrent.ConcurrentHashMap;

/**
 * @author: sugar
 * @date: 2023/11/4
 * @description:
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
