package edu.hubu.common;

import edu.hubu.common.PermName;
import edu.hubu.common.utils.MixAll;
import edu.hubu.common.utils.NameServerAddressUtil;
import edu.hubu.remoting.netty.RemotingUtil;
import lombok.extern.slf4j.Slf4j;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * @author: sugar
 * @date: 2023/5/24
 * @description:
 */
@Slf4j
public class BrokerConfig {

    private String nameSrv = NameServerAddressUtil.getNameServerAddress();

    //默认使用本地地址
    private String brokerIP1 = RemotingUtil.getLocalAddress();
    private String brokerIP2 = RemotingUtil.getLocalAddress();
    private String brokerClusterName = "DefaultCluster";
    private String brokerName = localHostname();
    private long brokerId = MixAll.MASTER_ID;
    private int brokerPermission = PermName.PERM_READABLE | PermName.PERM_WRITABLE;

    private int defaultTopicQueueNums = 8;
    private boolean autoCreateTopicEnable = true;

    private int sendMessageQueueCapacity = 10000;
    private int sendMessageThreadNums = 8;

    private int pullThreadQueueCapacity = 10000;
    private int pullMessageThreadNums = 8;

    private boolean fetchNameSrvByAddressServer = false;
    private int registerNameSrvPeriod = 1000 * 60;
    private boolean forceRegister = true;

    private int registerBrokerTimeoutMillis = 6000;
    private boolean compressedRegister = false;

    private boolean longPollingEnable = true;
    private int consumerManageThreadNums = 8;
    private int adminBrokerThreadNums = 8;
    private boolean autoCreateSubscriptionGroup = true;
    private boolean enablePropertyFilter = false;
    //重试时是否支持消息过滤
    private boolean filterSupportRetry;


    public static String localHostname(){
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            log.error("get local host name exception");
        }
        return "DEFAULT_BROKER";
    }


    public int getSendMessageQueueCapacity() {
        return sendMessageQueueCapacity;
    }


    public void setSendMessageQueueCapacity(int sendMessageQueueCapacity) {
        this.sendMessageQueueCapacity = sendMessageQueueCapacity;
    }

    public int getSendMessageThreadNums() {
        return sendMessageThreadNums;
    }

    public void setSendMessageThreadNums(int sendMessageThreadNums) {
        this.sendMessageThreadNums = sendMessageThreadNums;
    }

    public int getPullThreadQueueCapacity() {
        return pullThreadQueueCapacity;
    }

    public void setPullThreadQueueCapacity(int pullThreadQueueCapacity) {
        this.pullThreadQueueCapacity = pullThreadQueueCapacity;
    }

    public int getPullMessageThreadNums() {
        return pullMessageThreadNums;
    }

    public void setPullMessageThreadNums(int pullMessageThreadNums) {
        this.pullMessageThreadNums = pullMessageThreadNums;
    }

    public String getNameSrvAddress() {
        return nameSrv;
    }

    public void setNameSrv(String nameSrv) {
        this.nameSrv = nameSrv;
    }

    public boolean isFetchNameSrvByAddressServer() {
        return fetchNameSrvByAddressServer;
    }

    public void setFetchNameSrvByAddressServer(boolean fetchNameSrvByAddressServer) {
        this.fetchNameSrvByAddressServer = fetchNameSrvByAddressServer;
    }

    public int getRegisterNameSrvPeriod() {
        return registerNameSrvPeriod;
    }

    public void setRegisterNameSrvPeriod(int registerNameSrvPeriod) {
        this.registerNameSrvPeriod = registerNameSrvPeriod;
    }

    public boolean isForceRegister() {
        return forceRegister;
    }

    public void setForceRegister(boolean forceRegister) {
        this.forceRegister = forceRegister;
    }

    public int getBrokerPermission() {
        return brokerPermission;
    }

    public void setBrokerPermission(int brokerPermission) {
        this.brokerPermission = brokerPermission;
    }

    public String getBrokerIP1() {
        return brokerIP1;
    }

    public void setBrokerIP1(String brokerIP1) {
        this.brokerIP1 = brokerIP1;
    }

    public String getBrokerClusterName() {
        return brokerClusterName;
    }

    public void setBrokerClusterName(String brokerClusterName) {
        this.brokerClusterName = brokerClusterName;
    }

    public String getBrokerName() {
        return brokerName;
    }

    public void setBrokerName(String brokerName) {
        this.brokerName = brokerName;
    }

    public long getBrokerId() {
        return brokerId;
    }

    public void setBrokerId(long brokerId) {
        this.brokerId = brokerId;
    }

    public int getDefaultTopicQueueNums() {
        return defaultTopicQueueNums;
    }

    public void setDefaultTopicQueueNums(int defaultTopicQueueNums) {
        this.defaultTopicQueueNums = defaultTopicQueueNums;
    }

    public boolean isAutoCreateTopicEnable() {
        return autoCreateTopicEnable;
    }

    public void setAutoCreateTopicEnable(boolean autoCreateTopicEnable) {
        this.autoCreateTopicEnable = autoCreateTopicEnable;
    }

    public int getRegisterBrokerTimeoutMillis() {
        return registerBrokerTimeoutMillis;
    }

    public void setRegisterBrokerTimeoutMillis(int registerBrokerTimeoutMillis) {
        this.registerBrokerTimeoutMillis = registerBrokerTimeoutMillis;
    }

    public String getBrokerIP2() {
        return brokerIP2;
    }

    public void setBrokerIP2(String brokerIP2) {
        this.brokerIP2 = brokerIP2;
    }

    public boolean isCompressedRegister() {
        return compressedRegister;
    }

    public void setCompressedRegister(boolean compressedRegister) {
        this.compressedRegister = compressedRegister;
    }

    public boolean isLongPollingEnable() {
        return longPollingEnable;
    }

    public void setLongPollingEnable(boolean longPollingEnable) {
        this.longPollingEnable = longPollingEnable;
    }

    public int getConsumerManageThreadNums() {
        return consumerManageThreadNums;
    }

    public void setConsumerManageThreadNums(int consumerManageThreadNums) {
        this.consumerManageThreadNums = consumerManageThreadNums;
    }

    public int getAdminBrokerThreadNums() {
        return adminBrokerThreadNums;
    }

    public void setAdminBrokerThreadNums(int adminBrokerThreadNums) {
        this.adminBrokerThreadNums = adminBrokerThreadNums;
    }

    public boolean isAutoCreateSubscriptionGroup() {
        return autoCreateSubscriptionGroup;
    }

    public void setAutoCreateSubscriptionGroup(boolean autoCreateSubscriptionGroup) {
        this.autoCreateSubscriptionGroup = autoCreateSubscriptionGroup;
    }

    public boolean isEnablePropertyFilter() {
        return enablePropertyFilter;
    }

    public void setEnablePropertyFilter(boolean enablePropertyFilter) {
        this.enablePropertyFilter = enablePropertyFilter;
    }

    public boolean isFilterSupportRetry() {
        return filterSupportRetry;
    }

    public void setFilterSupportRetry(boolean filterSupportRetry) {
        this.filterSupportRetry = filterSupportRetry;
    }
}
