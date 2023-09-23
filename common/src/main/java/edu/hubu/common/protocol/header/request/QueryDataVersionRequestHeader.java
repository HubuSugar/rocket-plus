package edu.hubu.common.protocol.header.request;

import edu.hubu.remoting.netty.CustomCommandHeader;

/**
 * @author: sugar
 * @date: 2023/6/29
 * @description:
 */
public class QueryDataVersionRequestHeader implements CustomCommandHeader {
    private String clusterName;
    private String brokerAddress;
    private long brokerId;
    private String brokerName;

    public String getClusterName() {
        return clusterName;
    }

    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }

    public String getBrokerAddress() {
        return brokerAddress;
    }

    public void setBrokerAddress(String brokerAddress) {
        this.brokerAddress = brokerAddress;
    }

    public long getBrokerId() {
        return brokerId;
    }

    public void setBrokerId(long brokerId) {
        this.brokerId = brokerId;
    }

    public String getBrokerName() {
        return brokerName;
    }

    public void setBrokerName(String brokerName) {
        this.brokerName = brokerName;
    }

    @Override
    public boolean checkFields() {
        return false;
    }
}
