package edu.hubu.client.latency;

import edu.hubu.client.common.ThreadLocalIndex;
import edu.hubu.client.impl.TopicPublishInfo;
import edu.hubu.common.message.MessageQueue;
import lombok.extern.slf4j.Slf4j;

/**
 * @author: sugar
 * @date: 2023/6/4
 * @description:
 */
@Slf4j
public class MQFaultStrategy {

    private boolean sendLatencyFaultEnable = false;
    private final LatencyFaultTolerance<String> latencyFaultTolerance = new LatencyFaultToleranceImpl();

    public MessageQueue selectOneMessageQueue(final TopicPublishInfo topicPublishInfo, String lastBrokerName){
        if(sendLatencyFaultEnable){
            try{
                int index = topicPublishInfo.getSendWhichQueue().getAndIncrement();
                for(int i = 0; i < topicPublishInfo.getMessageQueues().size();i++){
                    int pos = Math.abs(index++) % topicPublishInfo.getMessageQueues().size();
                    if(pos < 0) pos = 0;
                    MessageQueue mq = topicPublishInfo.getMessageQueues().get(pos);
                    if(latencyFaultTolerance.isAvailable(mq.getBrokerName())){
                        if(lastBrokerName == null || lastBrokerName.equals(mq.getBrokerName())){
                            return mq;
                        }
                    }
                }

                final String notBestBroker = latencyFaultTolerance.pickOneAtLeast();
                int writeNums = topicPublishInfo.findQueueNumsByBrokerName(notBestBroker);
                if(writeNums > 0){
                   MessageQueue mq = topicPublishInfo.selectOneMessageQueue();
                    if(notBestBroker != null){
                        mq.setBrokerName(notBestBroker);
                        mq.setQueueId(topicPublishInfo.getSendWhichQueue().getAndIncrement() % writeNums);
                    }
                    return mq;
                }else{
                    latencyFaultTolerance.remove(notBestBroker);
                }
            }catch (Exception e){
                log.error("select one message queue exception", e);
            }
        }
        return topicPublishInfo.selectOneMessageQueue(lastBrokerName);
    }

    public boolean isSendLatencyFaultEnable() {
        return sendLatencyFaultEnable;
    }

    public void setSendLatencyFaultEnable(boolean sendLatencyFaultEnable) {
        this.sendLatencyFaultEnable = sendLatencyFaultEnable;
    }
}
