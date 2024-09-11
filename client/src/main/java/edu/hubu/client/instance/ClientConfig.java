package edu.hubu.client.instance;

import edu.hubu.common.utils.NameServerAddressUtil;
import edu.hubu.common.utils.UtilAll;
import edu.hubu.remoting.netty.common.RemotingUtil;
import edu.hubu.remoting.netty.protocol.LanguageCode;

/**
 * @author: sugar
 * @date: 2023/5/26
 * @description:
 */
public class ClientConfig {
    public static final String SEND_MESSAGE_WITH_VIP_CHANNEL = "sendMessageWithVipChannel";
    private static final String SPLIT = "@";
    private String nameServer = NameServerAddressUtil.getNameServerAddress();
    private String clientIp = RemotingUtil.getLocalAddress();
    private String instanceName = System.getProperty("rocketmq.client.name", "DEFAULT");
    private int clientCallbackExecutorThreads = Runtime.getRuntime().availableProcessors();
    protected String namespace;

    private int pollNameSrvInterval = 1000 * 30;
    private int heartbeatBrokerInterval = 1000 * 30;

    private int persistConsumerOffsetInterval = 1000 * 5;
    private long pullTimeDelayWhenException = 1000;

    private boolean unitMode = false;
    private String unitName;
    private boolean vipChannelEnable = Boolean.parseBoolean(System.getProperty(SEND_MESSAGE_WITH_VIP_CHANNEL, "false"));

    private LanguageCode languageCode = LanguageCode.JAVA;


    public String buildMQClientId(){
        StringBuilder sb = new StringBuilder();
        sb.append(getClientIp());
        sb.append(SPLIT);
        sb.append(getInstanceName());
        if(!UtilAll.isBlank(unitName)){
            sb.append(SPLIT).append(unitName);
        }
        return sb.toString();
    }

    public void changeInstanceNameToPID() {
        if(instanceName.equals("DEFAULT")){
            this.instanceName = String.valueOf(UtilAll.getPid());
        }
    }

    public void resetClientConfig(ClientConfig cc) {
        this.nameServer = cc.nameServer;
        this.clientIp = cc.clientIp;
        this.instanceName = cc.instanceName;
        this.clientCallbackExecutorThreads = cc.clientCallbackExecutorThreads;
        this.pollNameSrvInterval = cc.pollNameSrvInterval;
        this.heartbeatBrokerInterval = cc.heartbeatBrokerInterval;
        this.unitMode = cc.unitMode;
        this.unitName = cc.unitName;
        this.namespace = cc.namespace;
        this.languageCode = cc.languageCode;
    }

    public ClientConfig cloneClientConfig(){
        ClientConfig cc = new ClientConfig();
        cc.nameServer = this.nameServer;
        cc.clientIp = this.clientIp;
        cc.instanceName = this.instanceName;
        cc.clientCallbackExecutorThreads = this.clientCallbackExecutorThreads;
        cc.pollNameSrvInterval = this.pollNameSrvInterval;
        cc.heartbeatBrokerInterval = this.heartbeatBrokerInterval;
        cc.persistConsumerOffsetInterval = this.persistConsumerOffsetInterval;
        cc.pullTimeDelayWhenException = this.pullTimeDelayWhenException;
        cc.unitMode = this.unitMode;
        cc.unitName = this.unitName;
        cc.vipChannelEnable = this.vipChannelEnable;
        cc.namespace = this.namespace;
        cc.languageCode = this.languageCode;
        return cc;
    }


    public String getNameServer() {
        if(!UtilAll.isBlank(nameServer) && NameServerAddressUtil.NAMESRV_ENDPOINT_PATTERN.matcher(nameServer.trim()).matches()){
            return nameServer.substring(NameServerAddressUtil.ENDPOINT_PREFIX.length());
        }
        return nameServer;
    }

    public String getClientIp() {
        return clientIp;
    }

    public String getInstanceName() {
        return instanceName;
    }

    public void setInstanceName(String instanceName) {
        this.instanceName = instanceName;
    }

    public boolean isUnitMode() {
        return unitMode;
    }

    public void setUnitMode(boolean unitMode) {
        this.unitMode = unitMode;
    }

    public String getUnitName() {
        return unitName;
    }

    public void setUnitName(String unitName) {
        this.unitName = unitName;
    }

    public void setNameServer(String nameServer) {
        this.nameServer = nameServer;
    }

    public long getPollNameSrvInterval() {
        return pollNameSrvInterval;
    }

    public void setPollNameSrvInterval(int pollNameSrvInterval) {
        this.pollNameSrvInterval = pollNameSrvInterval;
    }

    public String getNamespace() {
        return namespace;
    }

    public void setNamespace(String namespace) {
        this.namespace = namespace;
    }

    public boolean isVipChannelEnable() {
        return vipChannelEnable;
    }

    public void setVipChannelEnable(boolean vipChannelEnable) {
        this.vipChannelEnable = vipChannelEnable;
    }

    public LanguageCode getLanguageCode() {
        return languageCode;
    }

    public void setLanguageCode(LanguageCode languageCode) {
        this.languageCode = languageCode;
    }

    public long getHeartbeatBrokerInterval() {
        return heartbeatBrokerInterval;
    }

    public void setHeartbeatBrokerInterval(int heartbeatBrokerInterval) {
        this.heartbeatBrokerInterval = heartbeatBrokerInterval;
    }
}
