package edu.hubu.client.instance;

import edu.hubu.common.utils.NameServerAddressUtil;
import edu.hubu.remoting.netty.RemotingUtil;
import io.netty.util.internal.StringUtil;

/**
 * @author: sugar
 * @date: 2023/5/26
 * @description:
 */
public class ClientConfig {
    public static final String SEND_MESSAGE_WITH_VIP_CHANNEL = "sendMessageWithVipChannel";
    private static final String SPLIT = "@";
    private String nameServer = NameServerAddressUtil.getNameServerAddress();
    private final String clientIp = RemotingUtil.getLocalAddress();
    private String instanceName = System.getProperty("rocketmq.client.name", "DEFAULT");

    private boolean unitMode = false;
    private String unitName;
    private boolean vipChannelEnable = Boolean.parseBoolean(System.getProperty(SEND_MESSAGE_WITH_VIP_CHANNEL, "false"));

    private long pollNameSrvInterval = 1000 * 30;
    protected String namespace;


    public String buildMQClientId(){
        StringBuilder sb = new StringBuilder();
        sb.append(getClientIp());
        sb.append(SPLIT);
        sb.append(getInstanceName());
        if(!StringUtil.isNullOrEmpty(unitName)){
            sb.append(SPLIT).append(unitName);
        }
        return sb.toString();
    }


    public String getNameServer() {
        if(!StringUtil.isNullOrEmpty(nameServer) && NameServerAddressUtil.NAMESRV_ENDPOINT_PATTERN.matcher(nameServer.trim()).matches()){
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

    public void setPollNameSrvInterval(long pollNameSrvInterval) {
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
}
