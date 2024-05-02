package edu.hubu.broker.subscription;

import edu.hubu.broker.starter.BrokerController;
import edu.hubu.common.ConfigManager;
import edu.hubu.common.DataVersion;
import edu.hubu.common.subcription.SubscriptionGroupConfig;
import edu.hubu.common.utils.MixAll;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.ConcurrentHashMap;

/**
 * @author: sugar
 * @date: 2023/12/2
 * @description:
 */
@Slf4j
public class SubscriptionGroupManager extends ConfigManager {

    //<consumerGroup, Object>
    private final ConcurrentHashMap<String, SubscriptionGroupConfig> subscriptionGroupTable = new ConcurrentHashMap<>(1024);
    private final DataVersion dataVersion = new DataVersion();
    private transient BrokerController brokerController;

    public SubscriptionGroupManager() {
        this.init();
    }

    public SubscriptionGroupManager(BrokerController brokerController) {
        this.brokerController = brokerController;
        this.init();
    }

    /**
     * 执行一些初始化的操作
     */
    private void init(){
        {

        }
        {

        }
    }

    public SubscriptionGroupConfig findSubscriptionGroupConfig(final String groupName){
        SubscriptionGroupConfig subscriptionGroupConfig = this.subscriptionGroupTable.get(groupName);
        if(subscriptionGroupConfig == null){
            if(this.brokerController.getBrokerConfig().isAutoCreateSubscriptionGroup() || MixAll.isSysConsumerGroup(groupName)){
                subscriptionGroupConfig = new SubscriptionGroupConfig();
                subscriptionGroupConfig.setGroupName(groupName);
                SubscriptionGroupConfig old = this.subscriptionGroupTable.putIfAbsent(groupName, subscriptionGroupConfig);
                if(old == null){
                    log.info("auto create a subscription group: {}", groupName);
                }
                this.dataVersion.nextVersion();
                this.persist();
            }
        }
        return subscriptionGroupConfig;
    }
}
