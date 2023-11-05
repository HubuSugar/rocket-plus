package edu.hubu.client.impl.consumer;

import edu.hubu.client.consumer.DefaultLitePullConsumer;
import edu.hubu.client.consumer.MessageQueueListener;
import edu.hubu.client.consumer.MessageSelector;
import edu.hubu.client.consumer.store.LocalFileOffsetStore;
import edu.hubu.client.consumer.store.OffsetStore;
import edu.hubu.client.consumer.store.RemoteBrokerOffsetStore;
import edu.hubu.client.exception.MQClientException;
import edu.hubu.client.hook.FilterMessageHook;
import edu.hubu.client.impl.rebalance.RebalanceImpl;
import edu.hubu.client.impl.rebalance.RebalanceLitePullImpl;
import edu.hubu.client.instance.MQClientInstance;
import edu.hubu.client.instance.MQClientManager;
import edu.hubu.common.ServiceState;
import edu.hubu.common.filter.FilterAPI;
import edu.hubu.common.message.MessageQueue;
import edu.hubu.common.protocol.heartbeat.MessageModel;
import edu.hubu.common.protocol.heartbeat.SubscriptionData;
import edu.hubu.remoting.netty.handler.RpcHook;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;

/**
 * @author: sugar
 * @date: 2023/10/28
 * @description:
 */
public class DefaultLitePullConsumerImpl implements MQConsumerInner{

    private static final String SUBSCRIPTION_CONFLICT_EXCEPTION_MSG = "subscribe and assign are exclusive";

    private final DefaultLitePullConsumer defaultLitePullConsumer;
    private final RpcHook rpcHook;
    private final ArrayList<FilterMessageHook> filterMessageHookList = new ArrayList<FilterMessageHook>();

    private volatile ServiceState serviceState = ServiceState.CREATE_JUST;

    protected MQClientInstance mqClientInstance;
    private PullAPIWrapper pullAPIWrapper;

    //defaultLitePullConsumer中指定了就是用指定的实现类,如果没有的话就根据MessageModel使用，广播模式使用local、集群模式使用remote
    private OffsetStore offsetStore;

    private RebalanceImpl rebalanceImpl = new RebalanceLitePullImpl(this);

    private SubscriptionType subscribeType = SubscriptionType.NONE;

    private ScheduledThreadPoolExecutor scheduledThreadPoolExecutor;

    // private ConcurrentHashMap<String, PullTaskImpl>

    private AssignedMessageQueue assignedMessageQueue = new AssignedMessageQueue();


    public DefaultLitePullConsumerImpl(DefaultLitePullConsumer defaultLitePullConsumer, RpcHook rpcHook) {
        this.defaultLitePullConsumer = defaultLitePullConsumer;
        this.rpcHook = rpcHook;

        //创建线程池
        this.scheduledThreadPoolExecutor = new ScheduledThreadPoolExecutor(defaultLitePullConsumer.getPullThreadNums(), new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                return new Thread(r,"pullMsgThread-" + defaultLitePullConsumer.getConsumerGroup());
            }
        });
    }

    @Override
    public void doRebalance() {
        if(rebalanceImpl != null){
            this.rebalanceImpl.doRebalance(false);
        }
    }

    @Override
    public void updateTopicSubscribeInfo(String topic, Set<MessageQueue> topicSubscribeInfo) {
        ConcurrentHashMap<String, SubscriptionData> subTable = this.rebalanceImpl.getSubscriptionInner();
        if(subTable != null){
            if(subTable.containsKey(topic)){
                this.rebalanceImpl.getTopicSubscribeTable().put(topic, topicSubscribeInfo);
            }
        }
    }

    public synchronized void start() throws MQClientException {
        switch (this.serviceState){
            case CREATE_JUST:

                this.serviceState = ServiceState.START_FAILED;

                //客户端工厂
                this.initMQClientFactory();

                //rebalance
                this.initRebalanceImpl();

                //消息拉取RPC API
                this.initPullAPIWrapper();

                //初始化消费偏移量管理
                this.initOffsetStore();

                this.mqClientInstance.start();

                serviceState = ServiceState.RUNNING;

                operateAfterRunning();
                break;
            case START_FAILED:
            case RUNNING:
            case SHUTDOWN_ALREADY:
                throw new MQClientException("consumer start failed", null);
            default:
                break;
        }
    }

    private void initMQClientFactory() throws MQClientException {
        this.mqClientInstance = MQClientManager.getInstance().getOrCreateInstance(this.defaultLitePullConsumer);
        boolean registerOK = this.mqClientInstance.registerConsumer(this.defaultLitePullConsumer.getConsumerGroup(), this);
        if(!registerOK){
            this.serviceState = ServiceState.CREATE_JUST;
            throw new MQClientException("the consumer group " + this.defaultLitePullConsumer.getConsumerGroup() + " has been registered before", null);
        }
    }

    private void initRebalanceImpl(){
        this.rebalanceImpl.setConsumerGroup(defaultLitePullConsumer.getConsumerGroup());
        this.rebalanceImpl.setMessageModel(defaultLitePullConsumer.getMessageModel());
        this.rebalanceImpl.setAllocateMessageQueueStrategy(defaultLitePullConsumer.getAllocateMessageQueueStrategy());
        this.rebalanceImpl.setMqClientInstance(this.mqClientInstance);
    }

    private void initPullAPIWrapper(){
        this.pullAPIWrapper = new PullAPIWrapper(this.mqClientInstance, defaultLitePullConsumer.getConsumerGroup(), isUnitMode());
        this.pullAPIWrapper.registerFilterMessageHook(filterMessageHookList);
    }

    private void initOffsetStore(){
        if(defaultLitePullConsumer.getOffsetStore() != null){
            this.offsetStore = defaultLitePullConsumer.getOffsetStore();
        }else{
            switch (this.defaultLitePullConsumer.getMessageModel()){
                case BROADCASTING:
                    this.offsetStore = new LocalFileOffsetStore(this.mqClientInstance, this.defaultLitePullConsumer.getConsumerGroup());
                    break;
                case CLUSTERING:
                    this.offsetStore = new RemoteBrokerOffsetStore(this.mqClientInstance, this.defaultLitePullConsumer.getConsumerGroup());
                    break;
                default:
                    break;
            }
            this.defaultLitePullConsumer.setOffsetStore(offsetStore);
        }
        this.offsetStore.load();
    }

    private void operateAfterRunning(){

    }


    public synchronized void subscribe(String topic, String subExpression) throws MQClientException {
        try {
            if (topic == null || "".equals(topic)) {
                throw new IllegalArgumentException("topic can not be null or empty");
            }
            setSubscribeType(SubscriptionType.SUBSCRIBE);
            SubscriptionData subscriptionData = FilterAPI.buildSubscriptionData(this.defaultLitePullConsumer.getConsumerGroup(), topic, subExpression);
            this.rebalanceImpl.getSubscriptionInner().put(topic, subscriptionData);
            this.defaultLitePullConsumer.setMessageQueueListener(new MessageQueueListenerImpl());
            if(serviceState == ServiceState.RUNNING){
                this.mqClientInstance.sendHeartbeatToAllBrokerWithLock();
                updateTopicSubscribeInfoWhenSubscriptionChanged();
            }

        } catch (Exception e) {
            throw new MQClientException("subscribe exception", e);
        }

    }

    private void updateTopicSubscribeInfoWhenSubscriptionChanged() {
    }

    public synchronized void subscribe(String topic, MessageSelector messageSelector){

    }

    @Override
    public boolean isUnitMode() {
        return this.defaultLitePullConsumer.isUnitMode();
    }

    public DefaultLitePullConsumer getDefaultLitePullConsumer() {
        return defaultLitePullConsumer;
    }

    public OffsetStore getOffsetStore() {
        return offsetStore;
    }

    public void setOffsetStore(OffsetStore offsetStore) {
        this.offsetStore = offsetStore;
    }


    private enum SubscriptionType{
        NONE, ASSIGN, SUBSCRIBE
    }


    private synchronized void setSubscribeType(SubscriptionType type){
        if(SubscriptionType.NONE == type){
            this.subscribeType = type;
        }else if(type != this.subscribeType){
            throw new IllegalStateException(SUBSCRIPTION_CONFLICT_EXCEPTION_MSG);
        }
    }

    private void updatedAssignedMessageQueue(String topic, Set<MessageQueue> mqDivided) {
        this.assignedMessageQueue.updateAssignedMessageQueue(topic, mqDivided);
    }

    private void updatePullTask(String topic, Collection<MessageQueue> mqSet){


        startPullTask(mqSet);
    }

    private void startPullTask(Collection<MessageQueue> mqSet){

    }
    
    class MessageQueueListenerImpl implements MessageQueueListener{
        @Override
        public void messageQueueChanged(String topic, Set<MessageQueue> mqAll, Set<MessageQueue> mqDivided) {
            MessageModel messageModel = defaultLitePullConsumer.getMessageModel();
            switch (messageModel){
                case BROADCASTING:
                    break;
                case CLUSTERING:
                    updatedAssignedMessageQueue(topic, mqDivided);
                    updatePullTask(topic, mqDivided);
                    break;
            }
        }
    }


}
