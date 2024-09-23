package edu.hubu.store;

import edu.hubu.common.protocol.heartbeat.SubscriptionData;

import java.nio.ByteBuffer;
import java.util.Map;

/**
 * @author: sugar
 * @date: 2023/12/9
 * @description:
 */
public class DefaultMessageFilter implements MessageFilter{

    private SubscriptionData subscriptionData;

    public DefaultMessageFilter(SubscriptionData subscriptionData) {
        this.subscriptionData = subscriptionData;
    }

    @Override
    public boolean isMatchedByConsumeQueue(Long tagsCode, ConsumeQueueExt.CqUnitExt cqUnitExt) {
        if(tagsCode == null || null == subscriptionData)
        return true;

        return true;
    }

    @Override
    public boolean isMatchedByCommitLog(ByteBuffer byteBuffer, Map<String, String> properties) {
        return true;
    }
}
