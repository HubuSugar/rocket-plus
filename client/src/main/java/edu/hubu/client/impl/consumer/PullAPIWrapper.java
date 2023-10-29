package edu.hubu.client.impl.consumer;

import edu.hubu.client.hook.FilterMessageHook;
import edu.hubu.client.instance.MQClientInstance;

import java.util.ArrayList;

/**
 * @author: sugar
 * @date: 2023/10/28
 * @description:
 */
public class PullAPIWrapper {

    private final MQClientInstance mqClientInstance;
    private final String consumerGroup;
    private final boolean unitMode;

    public PullAPIWrapper(MQClientInstance mqClientInstance, String consumerGroup, boolean unitMode) {
        this.mqClientInstance = mqClientInstance;
        this.consumerGroup = consumerGroup;
        this.unitMode = unitMode;
    }

    public void registerFilterMessageHook(ArrayList<FilterMessageHook> filterMessageHookList) {

    }

    public MQClientInstance getMqClientInstance() {
        return mqClientInstance;
    }

    public String getConsumerGroup() {
        return consumerGroup;
    }

    public boolean isUnitMode() {
        return unitMode;
    }


}
