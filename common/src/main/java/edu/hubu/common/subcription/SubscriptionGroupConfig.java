package edu.hubu.common.subcription;

import edu.hubu.common.utils.MixAll;
import lombok.EqualsAndHashCode;

/**
 * 订阅关系配置
 * 消费者组的一些配置信息
 * @author: sugar
 * @date: 2023/12/2
 * @description:
 */
@EqualsAndHashCode
public class SubscriptionGroupConfig {
    private String groupName;
    private boolean consumeEnable = true;
    private boolean consumeFromMinEnable = true;
    private boolean consumeBroadcastEnable = true;

    private int retryQueueNums = 1;
    private int retryMaxTimes = 16;

    private long brokerId = MixAll.MASTER_ID;
    private long whichBrokerWhenConsumeSlowly = 1;

    private boolean notifyConsumerIdsChangeEnable = true;

    public String getGroupName() {
        return groupName;
    }

    public void setGroupName(String groupName) {
        this.groupName = groupName;
    }

    public boolean isConsumeEnable() {
        return consumeEnable;
    }

    public void setConsumeEnable(boolean consumeEnable) {
        this.consumeEnable = consumeEnable;
    }

    public boolean isConsumeFromMinEnable() {
        return consumeFromMinEnable;
    }

    public void setConsumeFromMinEnable(boolean consumeFromMinEnable) {
        this.consumeFromMinEnable = consumeFromMinEnable;
    }

    public boolean isConsumeBroadcastEnable() {
        return consumeBroadcastEnable;
    }

    public void setConsumeBroadcastEnable(boolean consumeBroadcastEnable) {
        this.consumeBroadcastEnable = consumeBroadcastEnable;
    }

    public int getRetryQueueNums() {
        return retryQueueNums;
    }

    public void setRetryQueueNums(int retryQueueNums) {
        this.retryQueueNums = retryQueueNums;
    }

    public int getRetryMaxTimes() {
        return retryMaxTimes;
    }

    public void setRetryMaxTimes(int retryMaxTimes) {
        this.retryMaxTimes = retryMaxTimes;
    }

    public long getBrokerId() {
        return brokerId;
    }

    public void setBrokerId(long brokerId) {
        this.brokerId = brokerId;
    }

    public long getWhichBrokerWhenConsumeSlowly() {
        return whichBrokerWhenConsumeSlowly;
    }

    public void setWhichBrokerWhenConsumeSlowly(long whichBrokerWhenConsumeSlowly) {
        this.whichBrokerWhenConsumeSlowly = whichBrokerWhenConsumeSlowly;
    }

    public boolean isNotifyConsumerIdsChangeEnable() {
        return notifyConsumerIdsChangeEnable;
    }

    public void setNotifyConsumerIdsChangeEnable(boolean notifyConsumerIdsChangeEnable) {
        this.notifyConsumerIdsChangeEnable = notifyConsumerIdsChangeEnable;
    }
}
