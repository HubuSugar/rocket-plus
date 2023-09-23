package edu.hubu.common.protocol.header.request;

import edu.hubu.remoting.netty.CustomCommandHeader;

/**
 * @author: sugar
 * @date: 2023/6/11
 * @description:
 */
public class RegisterBrokerRequestHeader implements CustomCommandHeader {

    private String brokerName;
    private String brokerAddress;
    private String clusterName;
    private Long brokerId;

    private String  haServerAddr;
    private boolean compressed;

    private Integer crc32 = 0;

    @Override
    public boolean checkFields() {
        return false;
    }

    public String getBrokerName() {
        return brokerName;
    }

    public void setBrokerName(String brokerName) {
        this.brokerName = brokerName;
    }

    public String getBrokerAddress() {
        return brokerAddress;
    }

    public void setBrokerAddress(String brokerAddress) {
        this.brokerAddress = brokerAddress;
    }

    public String getClusterName() {
        return clusterName;
    }

    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }

    public Long getBrokerId() {
        return brokerId;
    }

    public void setBrokerId(Long brokerId) {
        this.brokerId = brokerId;
    }

    public String getHaServerAddr() {
        return haServerAddr;
    }

    public void setHaServerAddr(String haServerAddr) {
        this.haServerAddr = haServerAddr;
    }

    public Integer getCrc32() {
        return crc32;
    }

    public void setCrc32(Integer crc32) {
        this.crc32 = crc32;
    }

    public boolean isCompressed() {
        return compressed;
    }

    public void setCompressed(boolean compressed) {
        this.compressed = compressed;
    }
}
