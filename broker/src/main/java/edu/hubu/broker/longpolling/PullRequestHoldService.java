package edu.hubu.broker.longpolling;

import edu.hubu.broker.starter.BrokerController;

/**
 * @author: sugar
 * @date: 2023/7/15
 * @description:
 */
public class PullRequestHoldService {

    private final BrokerController brokerController;

    public PullRequestHoldService(BrokerController brokerController) {
        this.brokerController = brokerController;
    }
}
