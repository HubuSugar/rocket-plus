package edu.hubu.broker.filter;

import edu.hubu.common.protocol.heartbeat.SubscriptionData;
import edu.hubu.store.ConsumeQueueExt;
import edu.hubu.store.MessageFilter;

import java.nio.ByteBuffer;
import java.util.Map;

/**
 * @author: sugar
 * @date: 2023/12/9
 * @description:
 */
public class ExpressionMessageFilter implements MessageFilter {

    private final SubscriptionData subscriptionData;
    private final ConsumerFilterData consumerFilterData;
    private final ConsumerFilterManager consumerFilterManager;
    private final boolean bloomDataValid;

    public ExpressionMessageFilter(SubscriptionData subscriptionData,
                                   ConsumerFilterData consumerFilterData,
                                   ConsumerFilterManager consumerFilterManager) {
        this.subscriptionData = subscriptionData;
        this.consumerFilterData = consumerFilterData;
        this.consumerFilterManager = consumerFilterManager;

        bloomDataValid = false;
    }

    @Override
    public boolean isMatchedByConsumeQueue(Long tagsCode, ConsumeQueueExt.CqUnitExt cqUnitExt) {
        return false;
    }

    @Override
    public boolean isMatchedByCommitLog(ByteBuffer byteBuffer, Map<String, String> properties) {
        return false;
    }


    public SubscriptionData getSubscriptionData() {
        return subscriptionData;
    }

    public ConsumerFilterData getConsumerFilterData() {
        return consumerFilterData;
    }

    public ConsumerFilterManager getConsumerFilterManager() {
        return consumerFilterManager;
    }

    public boolean isBloomDataValid() {
        return bloomDataValid;
    }
}
