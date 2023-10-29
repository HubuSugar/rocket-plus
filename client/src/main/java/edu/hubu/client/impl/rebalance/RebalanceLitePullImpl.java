package edu.hubu.client.impl.rebalance;

import edu.hubu.client.consumer.strategy.AllocateMessageQueueStrategy;
import edu.hubu.client.impl.consumer.DefaultLitePullConsumerImpl;
import edu.hubu.client.instance.MQClientInstance;
import edu.hubu.common.protocol.heartbeat.MessageModel;

/**
 * @author: sugar
 * @date: 2023/10/28
 * @description:
 */
public class RebalanceLitePullImpl extends RebalanceImpl{

    private final DefaultLitePullConsumerImpl litePullConsumer;


    public RebalanceLitePullImpl(final DefaultLitePullConsumerImpl litePullConsumer) {
        this(null, null, null, null, litePullConsumer);
    }

    public RebalanceLitePullImpl(String consumerGroup, MessageModel messageModel, AllocateMessageQueueStrategy allocateMessageQueueStrategy,
                                 MQClientInstance mqClientInstance, DefaultLitePullConsumerImpl litePullConsumer) {
        super(consumerGroup, messageModel, allocateMessageQueueStrategy, mqClientInstance);
        this.litePullConsumer = litePullConsumer;
    }

}
