package edu.hubu.common.protocol.route;

import java.util.HashMap;

/**
 * @author: sugar
 * @date: 2023/6/2
 * @description:
 */
public class BrokerData implements Comparable<BrokerData>{
    private String cluster;
    private String brokerName;
    //<brokerId, brokerAddress>
    private HashMap<Long, String> brokerAddrTable = new HashMap<>();

    public String getCluster() {
        return cluster;
    }

    public BrokerData setCluster(String cluster) {
        this.cluster = cluster;
        return this;
    }

    public String getBrokerName() {
        return brokerName;
    }

    public BrokerData setBrokerName(String brokerName) {
        this.brokerName = brokerName;
        return this;
    }

    public HashMap<Long, String> getBrokerAddrTable() {
        return brokerAddrTable;
    }

    public BrokerData setBrokerAddrTable(HashMap<Long, String> brokerAddrTable) {
        this.brokerAddrTable = brokerAddrTable;
        return this;
    }

    @Override
    public int compareTo(BrokerData o) {
        return 0;
    }
}
