package edu.hubu.broker.filter;

import edu.hubu.common.filter.ExpressionType;
import edu.hubu.common.protocol.heartbeat.SubscriptionData;
import edu.hubu.store.ConsumeQueueExt;
import edu.hubu.store.MessageFilter;
import lombok.extern.slf4j.Slf4j;

import java.nio.ByteBuffer;
import java.util.Map;

/**
 * @author: sugar
 * @date: 2023/12/9
 * @description:
 */
@Slf4j
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
        if(subscriptionData == null) return true;
        if(subscriptionData.isClassFilterMode()) return true;

        if(ExpressionType.isTagType(subscriptionData.getExpressionType())){  //by tags code
            if(tagsCode == null){
                return true;
            }

            if(subscriptionData.getSubString().equals(SubscriptionData.SUB_ALL)){
                return true;
            }

            return subscriptionData.getCodeSet().contains(tagsCode.intValue());
        }else {
            if(consumerFilterData == null || consumerFilterData.getExpression() == null ||
                consumerFilterData.getCompiledExpression() == null || consumerFilterData.getBloomFilterData() == null){
                return true;
            }

            if(cqUnitExt == null || consumerFilterData.isMsgInLive(cqUnitExt.getMsgStoreTime())){
                log.debug("Pulled matched because not in live: {}, {}", consumerFilterData, cqUnitExt);
                return true;
            }


        }

        return true;
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
