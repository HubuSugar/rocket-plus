package edu.hubu.client.impl;

/**
 * @author: sugar
 * @date: 2023/11/2
 * @description: broker查询订阅的broker集群
 */
public class FindBrokerResult {
    private final String brokerAddress;
    private final boolean slave;
    private final int brokerVersion;

    public FindBrokerResult(String brokerAddress, boolean slave) {
        this.brokerAddress = brokerAddress;
        this.slave = slave;
        this.brokerVersion = 0;
    }

    public FindBrokerResult(String brokerAddress, boolean slave, int brokerVersion) {
        this.brokerAddress = brokerAddress;
        this.slave = slave;
        this.brokerVersion = brokerVersion;
    }

    public String getBrokerAddress() {
        return brokerAddress;
    }

    public boolean isSlave() {
        return slave;
    }

    public int getBrokerVersion() {
        return brokerVersion;
    }
}
