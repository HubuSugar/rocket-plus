package edu.hubu.broker.filter;

import edu.hubu.common.protocol.heartbeat.SubscriptionData;
import edu.hubu.store.ConsumeQueueExt;

import java.nio.ByteBuffer;
import java.util.Map;

/**
 * @author: sugar
 * @date: 2023/12/9
 * @description:
 */
public class ExpressionForRetryMessageFilter extends ExpressionMessageFilter{

    public ExpressionForRetryMessageFilter(SubscriptionData subscriptionData, ConsumerFilterData consumerFilterData, ConsumerFilterManager consumerFilterManager) {
        super(subscriptionData, consumerFilterData, consumerFilterManager);
    }

    @Override
    public boolean isMatchedByConsumeQueue(Long tagsCode, ConsumeQueueExt.CqUnitExt cqUnitExt) {
        return super.isMatchedByConsumeQueue(tagsCode, cqUnitExt);
    }

    @Override
    public boolean isMatchedByCommitLog(ByteBuffer byteBuffer, Map<String, String> properties) {
        return super.isMatchedByCommitLog(byteBuffer, properties);
    }
}
