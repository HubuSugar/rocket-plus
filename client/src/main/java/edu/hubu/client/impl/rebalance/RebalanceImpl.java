package edu.hubu.client.impl.rebalance;

import edu.hubu.client.consumer.strategy.AllocateMessageQueueStrategy;
import edu.hubu.client.impl.consumer.ProcessQueue;
import edu.hubu.client.instance.MQClientInstance;
import edu.hubu.common.message.MessageQueue;
import edu.hubu.common.protocol.heartbeat.MessageModel;
import edu.hubu.common.protocol.heartbeat.SubscriptionData;
import edu.hubu.common.utils.MixAll;
import lombok.extern.slf4j.Slf4j;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author: sugar
 * @date: 2023/10/28
 * @description:
 */
@Slf4j
public abstract class RebalanceImpl {

    private final ConcurrentHashMap<MessageQueue, ProcessQueue> processQueueTable = new ConcurrentHashMap<>(64);
    private final ConcurrentHashMap<String, Set<MessageQueue>> topicSubscribeTable = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, SubscriptionData> subscriptionInner = new ConcurrentHashMap<>();

    protected String consumerGroup;
    protected MessageModel messageModel;
    protected AllocateMessageQueueStrategy allocateMessageQueueStrategy;
    protected MQClientInstance mqClientInstance;

    public RebalanceImpl(String consumerGroup, MessageModel messageModel, AllocateMessageQueueStrategy allocateMessageQueueStrategy, MQClientInstance mqClientInstance) {
        this.consumerGroup = consumerGroup;
        this.messageModel = messageModel;
        this.allocateMessageQueueStrategy = allocateMessageQueueStrategy;
        this.mqClientInstance = mqClientInstance;
    }

    public void doRebalance(boolean isOrder){
        Map<String, SubscriptionData> subTable = this.getSubscriptionInner();
        if(subTable != null){
            for (Map.Entry<String, SubscriptionData> entry : subTable.entrySet()) {
                String topic = entry.getKey();
                try{
                    this.rebalanceByTopic(topic, isOrder);
                }catch (Exception e){
                    if(!topic.startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)){
                        log.error("rebalance by topic exception");
                    }
                }
            }
        }
    }

    private void rebalanceByTopic(final String topic, boolean isOrder){
        switch (messageModel){
            case BROADCASTING:{

                break;
            }
            case CLUSTERING:{
                Set<MessageQueue> mqSet = this.topicSubscribeTable.get(topic);
                List<String> cidAll = this.mqClientInstance.findConsumerIdList(topic, consumerGroup);
                if(mqSet == null){
                    if(!topic.startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)){
                        log.warn("doRebalance the topic {} does not exist, consumerGroup: {}", topic, consumerGroup);
                    }
                }

                if(cidAll == null){
                    log.warn("doRebalance get consumer id list failed, topic:{}, consumerGroup:{}", topic, consumerGroup);
                }

                if(mqSet != null && cidAll != null){
                    List<MessageQueue> mqAll = new ArrayList<>(mqSet);

                    Collections.sort(mqAll);
                    Collections.sort(cidAll);

                    AllocateMessageQueueStrategy allocateStrategy = this.allocateMessageQueueStrategy;

                    List<MessageQueue> allocateResult = null;

                    try{
                        allocateResult = allocateStrategy.allocate(this.consumerGroup, this.mqClientInstance.getClientId(),
                                mqAll, cidAll);

                    }catch (Throwable e){
                        log.error("rebalance by topic allocate message queue exception, strategy name: {}", allocateStrategy.getName(), e);
                        return;
                    }

                    Set<MessageQueue> allocateResultSet = new HashSet<>();
                    if(allocateResult != null){
                        allocateResultSet.addAll(allocateResult);
                    }

                    boolean changed = this.updateProcessQueueTableInRebalance(topic, allocateResultSet, isOrder);
                    if(changed){
                        log.warn("rebalance result changed, strategy name:{}, group:{}, topic:{},clientId:{}, mqAllSize:{}, cidAllSize:{}, allocateResult size:{}, allocateResult:{}", allocateStrategy.getName(),
                                consumerGroup, topic, this.mqClientInstance.getClientId(),mqSet.size(), cidAll.size(), allocateResultSet.size(), allocateResultSet);
                        this.messageQueueChanged(topic, mqSet, allocateResultSet);
                    }

                }

                break;
            }
            default:
                break;

        }

    }

    public abstract void messageQueueChanged(final String topic, final Set<MessageQueue> mqSet,final Set<MessageQueue> allocateResultSet);



    private boolean updateProcessQueueTableInRebalance(String topic, Set<MessageQueue> allocateResultSet, boolean isOrder) {
        return false;
    }

    public ConcurrentHashMap<MessageQueue, ProcessQueue> getProcessQueueTable() {
        return processQueueTable;
    }

    public ConcurrentHashMap<String, Set<MessageQueue>> getTopicSubscribeTable() {
        return topicSubscribeTable;
    }

    public ConcurrentHashMap<String, SubscriptionData> getSubscriptionInner() {
        return subscriptionInner;
    }

    public String getConsumerGroup() {
        return consumerGroup;
    }

    public void setConsumerGroup(String consumerGroup) {
        this.consumerGroup = consumerGroup;
    }

    public MessageModel getMessageModel() {
        return messageModel;
    }

    public void setMessageModel(MessageModel messageModel) {
        this.messageModel = messageModel;
    }

    public AllocateMessageQueueStrategy getAllocateMessageQueueStrategy() {
        return allocateMessageQueueStrategy;
    }

    public void setAllocateMessageQueueStrategy(AllocateMessageQueueStrategy allocateMessageQueueStrategy) {
        this.allocateMessageQueueStrategy = allocateMessageQueueStrategy;
    }

    public MQClientInstance getMqClientInstance() {
        return mqClientInstance;
    }

    public void setMqClientInstance(MQClientInstance mqClientInstance) {
        this.mqClientInstance = mqClientInstance;
    }
}
