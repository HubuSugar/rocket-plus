package edu.hubu.client.impl;

import edu.hubu.client.exception.MQClientException;
import edu.hubu.client.instance.MQClientInstance;
import edu.hubu.common.message.MessageQueue;

/**
 * @author: sugar
 * @date: 2023/11/5
 * @description:
 */
public class MQAdminImpl {

    private final MQClientInstance mqClientInstance;
    private long timeoutMillis = 6000;

    public MQAdminImpl(MQClientInstance mqClientInstance) {
        this.mqClientInstance = mqClientInstance;
    }

    public long maxOffset(MessageQueue mq) throws MQClientException {
        String brokerAddr = this.mqClientInstance.findBrokerAddressInPublish(mq.getBrokerName());
        if(brokerAddr == null){
            this.mqClientInstance.updateTopicInfoFromNameServer(mq.getTopic());
            brokerAddr = this.mqClientInstance.findBrokerAddressInPublish(mq.getBrokerName());
        }

        if(brokerAddr != null){
            try{
                return this.mqClientInstance.getMqClientAPI().getMaxOffset(brokerAddr, mq.getTopic(), mq.getQueueId(), timeoutMillis);
            }catch (Exception e){
                throw new MQClientException("invoke the broker " + mq.getBrokerName()+" failed", e);
            }
        }
        throw new MQClientException("the broker " + mq.getBrokerName()+ " does not exist", null);
    }


    public MQClientInstance getMqClientInstance() {
        return mqClientInstance;
    }
}
