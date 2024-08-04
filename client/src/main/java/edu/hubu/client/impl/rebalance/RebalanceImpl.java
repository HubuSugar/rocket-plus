package edu.hubu.client.impl.rebalance;

import edu.hubu.client.consumer.strategy.AllocateMessageQueueStrategy;
import edu.hubu.client.impl.FindBrokerResult;
import edu.hubu.client.impl.consumer.ProcessQueue;
import edu.hubu.client.impl.consumer.PullRequest;
import edu.hubu.client.instance.MQClientInstance;
import edu.hubu.common.message.MessageQueue;
import edu.hubu.common.protocol.body.LockBatchRequestBody;
import edu.hubu.common.protocol.heartbeat.ConsumeType;
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

    protected final ConcurrentHashMap<MessageQueue, ProcessQueue> processQueueTable = new ConcurrentHashMap<>(64);
    protected final ConcurrentHashMap<String, Set<MessageQueue>> topicSubscribeTable = new ConcurrentHashMap<>();
    protected final ConcurrentHashMap<String, SubscriptionData> subscriptionInner = new ConcurrentHashMap<>();

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


    public boolean lock(MessageQueue mq) {
        FindBrokerResult findBrokerResult = this.mqClientInstance.findBrokerAddressInSubscribe(mq.getBrokerName(), MixAll.MASTER_ID, true);

        if(findBrokerResult != null){
            LockBatchRequestBody requestBody = new LockBatchRequestBody();
            requestBody.setConsumerGroup(this.consumerGroup);
            requestBody.setClientId(this.mqClientInstance.getClientId());
            requestBody.getMqSet().add(mq);

            try{
                Set<MessageQueue> lockedMq = this.mqClientInstance.getMqClientAPI().lockBatchMQ(findBrokerResult.getBrokerAddress(), requestBody, 1000);
                for (MessageQueue messageQueue : lockedMq) {
                    ProcessQueue pq = this.processQueueTable.get(messageQueue);
                    if(pq != null){
                        pq.setLocked(true);
                        pq.setLastLockTimestamp(System.currentTimeMillis());
                    }
                }

                boolean lockOk = lockedMq.contains(mq);
                log.info("lock mq {}, consumerGroup:{}, mq:{}", lockOk ? "True": "False", consumerGroup, mq);
                return lockOk;
            }catch (Exception e){
                log.error("lock mq exception, {}", mq, e);
            }

        }

        return false;
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
                //rpc
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

    public abstract boolean removeUnnecessaryMessageQueue(MessageQueue mq, ProcessQueue pq);

    public abstract ConsumeType consumeType();

    public abstract void removeDirtyOffset(MessageQueue mq);

    public abstract long computePullFromWhere(MessageQueue mq);

    public abstract void dispatchPullRequest(List<PullRequest> pullRequests);


    /**
     * rebalance时更新processQueue
     * @param topic
     * @param mqSet 分配到的messageQueue集合
     * @param isOrder
     * @return
     */
    private boolean updateProcessQueueTableInRebalance(String topic, Set<MessageQueue> mqSet, boolean isOrder) {
        boolean changed = false;
        Iterator<Map.Entry<MessageQueue, ProcessQueue>> it = this.processQueueTable.entrySet().iterator();
        while(it.hasNext()){
            Map.Entry<MessageQueue, ProcessQueue> next = it.next();
            MessageQueue mq = next.getKey();
            ProcessQueue pq = next.getValue();
            if(mq.getTopic().equals(topic)){
                if(!mqSet.contains(mq)){  //没有分配到当前这个messageQueue, 所以先把processQueue下线
                    pq.setDropped(true);
                    if(this.removeUnnecessaryMessageQueue(mq, pq)){
                        it.remove();
                        changed = true;
                        log.warn("doRebalance, remove unnecessary message queue, consumerGroup:{}, mq:{}", consumerGroup, mq);
                    }
                }else if(pq.isPullExpired()){
                    switch (this.consumeType()){
                        case CONSUME_PASSIVELY:
                            pq.setDropped(true);
                            if(this.removeUnnecessaryMessageQueue(mq, pq)){
                                it.remove();
                                changed = true;
                                log.error("[BUG] doRebalance, consumerGroup:{}, remove unnecessary queue, {}, but pull is pause, so try to fix it", consumerGroup, mq);
                            }
                            break;
                        case CONSUME_ACTIVELY:
                        default:
                            break;
                    }
                }

            }
        }

        List<PullRequest> pullRequests = new ArrayList<>();
        for (MessageQueue mq : mqSet) {
            if(!this.processQueueTable.containsKey(mq)){
                if(isOrder && !this.lock(mq)){
                    log.warn("doRebalance, {} add a new message queue failed, {} because lock failed", consumerGroup, mq);
                    continue;
                }

                this.removeDirtyOffset(mq);
                ProcessQueue pq = new ProcessQueue();
                long nextOffset = this.computePullFromWhere(mq);
                if(nextOffset >= 0){
                    ProcessQueue prev = this.processQueueTable.putIfAbsent(mq, pq);
                    if(prev != null){
                        log.info("doRebalance consumerGroup:{}, mq already exists, mq:{}", consumerGroup, mq);
                    }else{
                        log.info("doRebalance consumerGroup:{}, add a new mq: {}", consumerGroup, mq);
                        PullRequest pullRequest = new PullRequest();
                        pullRequest.setConsumerGroup(consumerGroup);
                        pullRequest.setProcessQueue(pq);
                        pullRequest.setMessageQueue(mq);
                        pullRequest.setNextOffset(nextOffset);
                        pullRequests.add(pullRequest);
                        changed = true;
                    }
                }else{
                    log.warn("doRebalance {}, add a new mq failed:{}", consumerGroup, mq);
                }
            }
        }

        this.dispatchPullRequest(pullRequests);

        return changed;
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
