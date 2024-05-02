package edu.hubu.broker.filter;

import edu.hubu.broker.starter.BrokerController;

/**
 * @author: sugar
 * @date: 2023/12/3
 * @description:
 */
public class ConsumerFilterManager {

    private transient BrokerController brokerController;

    public ConsumerFilterManager() {
    }

    public ConsumerFilterManager(BrokerController brokerController) {
        this.brokerController = brokerController;
    }

    public static ConsumerFilterData build(String topic, String consumerGroup, String subscription, String expressionType, Long subVersion) {
        return null;
    }
}
