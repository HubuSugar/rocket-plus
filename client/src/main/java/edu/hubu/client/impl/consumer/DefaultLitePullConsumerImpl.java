package edu.hubu.client.impl.consumer;

import edu.hubu.client.consumer.*;
import edu.hubu.client.consumer.store.LocalFileOffsetStore;
import edu.hubu.client.consumer.store.OffsetStore;
import edu.hubu.client.consumer.store.RemoteBrokerOffsetStore;
import edu.hubu.client.exception.MQClientException;
import edu.hubu.client.hook.FilterMessageHook;
import edu.hubu.client.impl.CommunicationMode;
import edu.hubu.client.impl.rebalance.RebalanceImpl;
import edu.hubu.client.impl.rebalance.RebalanceLitePullImpl;
import edu.hubu.client.instance.MQClientInstance;
import edu.hubu.client.instance.MQClientManager;
import edu.hubu.common.ServiceState;
import edu.hubu.common.consumer.ConsumeFromWhere;
import edu.hubu.common.exception.broker.MQBrokerException;
import edu.hubu.common.filter.ExpressionType;
import edu.hubu.common.filter.FilterAPI;
import edu.hubu.common.message.MessageExt;
import edu.hubu.common.message.MessageQueue;
import edu.hubu.common.protocol.heartbeat.ConsumeType;
import edu.hubu.common.protocol.heartbeat.MessageModel;
import edu.hubu.common.protocol.heartbeat.SubscriptionData;
import edu.hubu.common.sysFlag.PullSysFlag;
import edu.hubu.remoting.netty.exception.RemotingException;
import edu.hubu.remoting.netty.handler.RpcHook;
import lombok.extern.slf4j.Slf4j;

import java.util.*;
import java.util.concurrent.*;

/**
 * @author: sugar
 * @date: 2023/10/28
 * @description:
 */
@Slf4j
public class DefaultLitePullConsumerImpl implements MQConsumerInner{

    private static final long PULL_TIME_DELAY_WHEN_PAUSE = 1000;
    private static final long PULL_TIME_DELAY_WHEN_FLOW_CONTROL = 50;
    private static final String SUBSCRIPTION_CONFLICT_EXCEPTION_MSG = "subscribe and assign are exclusive";

    private final DefaultLitePullConsumer defaultLitePullConsumer;
    private final RpcHook rpcHook;
    private final ArrayList<FilterMessageHook> filterMessageHookList = new ArrayList<>();

    private volatile ServiceState serviceState = ServiceState.CREATE_JUST;

    protected MQClientInstance mqClientInstance;
    private PullAPIWrapper pullAPIWrapper;

    //defaultLitePullConsumer中指定了就是用指定的实现类,如果没有的话就根据MessageModel使用，广播模式使用local、集群模式使用remote
    private OffsetStore offsetStore;

    private RebalanceImpl rebalanceImpl = new RebalanceLitePullImpl(this);

    private SubscriptionType subscribeType = SubscriptionType.NONE;
    private long pullDelayTimeMillisWhenException = 1000;

    private ScheduledThreadPoolExecutor scheduledThreadPoolExecutor;

    private final ConcurrentHashMap<MessageQueue, PullTaskImpl> taskTable = new ConcurrentHashMap<>();

    private AssignedMessageQueue assignedMessageQueue = new AssignedMessageQueue();
    private final BlockingQueue<ConsumeRequest> consumeRequestCache = new LinkedBlockingQueue<>();

    private Map<String, TopicMessageQueueChangeListener> topicMessageQueueListenerMap = new HashMap<String, TopicMessageQueueChangeListener>();

    private long consumeRequestFlowControlTimes = 0L;
    private long queueFlowControlTimes = 0L;
    private long queueMaxSpanFlowControlTimes = 0L;
    private long nextAutoCommitDeadline = -1;

    private final MessageQueueLock messageQueueLock = new MessageQueueLock();


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

                startScheduledTask();

                serviceState = ServiceState.RUNNING;

                log.info("the consumer [{}] start OK", this.defaultLitePullConsumer.getConsumerGroup());

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

    public void startScheduledTask(){
        this.scheduledThreadPoolExecutor.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                fetchTopicMessageQueuesAndCompare();
            }
        }, 1000 * 10, this.getDefaultLitePullConsumer().getTopicMetadataCheckIntervalMillis(), TimeUnit.MILLISECONDS);
    }

    private synchronized void fetchTopicMessageQueuesAndCompare(){
        for (Map.Entry<String, TopicMessageQueueChangeListener> entry : this.topicMessageQueueListenerMap.entrySet()) {
            String key = entry.getKey();

        }
    }

    private void operateAfterRunning(){
        if(this.subscribeType == SubscriptionType.SUBSCRIBE){
            updateTopicSubscribeInfoWhenSubscriptionChanged();
        }

        //assigned

        //
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
            this.assignedMessageQueue.setRebalanceImpl(rebalanceImpl);
            if(serviceState == ServiceState.RUNNING){
                this.mqClientInstance.sendHeartbeatToAllBrokerWithLock();
                updateTopicSubscribeInfoWhenSubscriptionChanged();
            }

        } catch (Exception e) {
            throw new MQClientException("subscribe exception", e);
        }

    }

    public synchronized List<MessageExt> poll(long timeout) {
        try {
            checkServiceState();
            if(timeout < 0){
                throw new IllegalArgumentException("timeout must not be negative");
            }
            if(defaultLitePullConsumer.isAutoCommit()){
                maybeAutoCommit();
            }

            long endTime = System.currentTimeMillis() + timeout;
            ConsumeRequest consumeRequest = consumeRequestCache.poll(endTime - System.currentTimeMillis(), TimeUnit.MILLISECONDS);

            if(endTime - System.currentTimeMillis() > 0){
                while(consumeRequest != null && consumeRequest.processQueue.isDropped()){
                    consumeRequest = consumeRequestCache.poll(endTime - System.currentTimeMillis(), TimeUnit.MILLISECONDS);
                    if(endTime - System.currentTimeMillis() <= 0){
                        break;
                    }
                }
            }

            if(consumeRequest != null && !consumeRequest.processQueue.isDropped()){
                List<MessageExt> messageExts = consumeRequest.messageExts;
                long offset = consumeRequest.getProcessQueue().removeMessage(messageExts);
                this.assignedMessageQueue.updateConsumeOffset(consumeRequest.getMessageQueue(), offset);
                //if namespace is not null, reset topic without namespace
                this.resetTopic(messageExts);
                return messageExts;
            }

        }catch (InterruptedException ignored){

        }
        return Collections.emptyList();
    }

    private void resetTopic(List<MessageExt> messageExts) {

    }

    private void maybeAutoCommit() {
        long now = System.currentTimeMillis();
        if(now >= nextAutoCommitDeadline){
            commitAll();
            nextAutoCommitDeadline = now + defaultLitePullConsumer.getAutoCommitIntervalMillis();
        }
    }

    private synchronized void commitAll() {
        try{
            for (MessageQueue messageQueue : assignedMessageQueue.getAssignedMessageQueues()) {
               long consumerOffset = assignedMessageQueue.getConsumerOffset(messageQueue);
                if(consumerOffset != -1){
                    ProcessQueue processQueue = assignedMessageQueue.getProcessQueue(messageQueue);
                    if(processQueue != null && !processQueue.isDropped()){
                        updateConsumeOffset(messageQueue, consumerOffset);
                    }
                }
            }

        }catch (Exception e){
            log.error("An error occurred when update consumer offset automatically");
        }

    }

    public void updateConsumeOffset(MessageQueue messageQueue, long consumerOffset) {
        this.checkServiceState();
        this.offsetStore.updateOffset(messageQueue, consumerOffset, false);
    }

    private void updateTopicSubscribeInfoWhenSubscriptionChanged() {
        ConcurrentHashMap<String, SubscriptionData> subTable = this.rebalanceImpl.getSubscriptionInner();
        if(subTable != null){
            for (final Map.Entry<String, SubscriptionData> entry : subTable.entrySet()) {
                final String topic = entry.getKey();
                this.mqClientInstance.updateTopicInfoFromNameServer(topic);
            }
        }
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
        if(SubscriptionType.NONE == this.subscribeType){
            this.subscribeType = type;
        }else if(type != this.subscribeType){
            throw new IllegalStateException(SUBSCRIPTION_CONFLICT_EXCEPTION_MSG);
        }
    }

    private void updatedAssignedMessageQueue(String topic, Set<MessageQueue> mqDivided) {
        this.assignedMessageQueue.updateAssignedMessageQueue(topic, mqDivided);
    }

    private void updatePullTask(String topic, Collection<MessageQueue> mqSet) {
        Iterator<Map.Entry<MessageQueue, PullTaskImpl>> it = this.taskTable.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<MessageQueue, PullTaskImpl> next = it.next();
            if (next.getKey().getTopic().equals(topic)) {
                if (!mqSet.contains(next.getKey())) {
                    next.getValue().setCancelled(true);
                    it.remove();
                }
            }
        }
        startPullTask(mqSet);
    }

    /**
     * 启动消息拉取任务
     * @param mqSet
     */
    private void startPullTask(Collection<MessageQueue> mqSet){
        for (MessageQueue mq : mqSet) {
            if(!this.taskTable.containsKey(mq)){
                PullTaskImpl pullTask = new PullTaskImpl(mq);
                this.taskTable.put(mq, pullTask);
                this.scheduledThreadPoolExecutor.schedule(pullTask, 0, TimeUnit.MILLISECONDS);
            }
        }
    }

    @Override
    public String groupName() {
        return this.defaultLitePullConsumer.getConsumerGroup();
    }

    @Override
    public MessageModel messageModel() {
        return this.defaultLitePullConsumer.getMessageModel();
    }

    @Override
    public ConsumeType consumeType() {
        return ConsumeType.CONSUME_ACTIVELY;
    }

    @Override
    public ConsumeFromWhere consumeFromWhere() {
        return ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET;
    }

    @Override
    public Set<SubscriptionData> subscriptions() {
        return new HashSet<>(this.rebalanceImpl.getSubscriptionInner().values());
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

    public class PullTaskImpl implements Runnable{

        private final MessageQueue messageQueue;
        private volatile boolean cancelled = false;

        public PullTaskImpl(MessageQueue messageQueue) {
            this.messageQueue = messageQueue;
        }

        @Override
        public void run() {

            if(!this.isCancelled()){
                if(assignedMessageQueue.isPause(messageQueue)){
                    scheduledThreadPoolExecutor.schedule(this, PULL_TIME_DELAY_WHEN_PAUSE, TimeUnit.MILLISECONDS);
                    log.debug("message queue:{} has been paused", messageQueue);
                    return;
                }

                ProcessQueue processQueue = assignedMessageQueue.getProcessQueue(messageQueue);
                //判断processQueue是否在线
                if(processQueue == null || processQueue.isDropped()){
                    log.warn("the message queue can not be able to poll, {}", messageQueue);
                    return;
                }

                if(consumeRequestCache.size() * defaultLitePullConsumer.getPullBatchSize() > defaultLitePullConsumer.getPullThresholdForAll()){
                    scheduledThreadPoolExecutor.schedule(this, PULL_TIME_DELAY_WHEN_FLOW_CONTROL, TimeUnit.MILLISECONDS);
                    if(consumeRequestFlowControlTimes++ % 1000 == 0){
                        log.warn("the consume request count exceed threshold, {}, so do flow control, {}, flow control times:{}",defaultLitePullConsumer.getPullThresholdForAll(), consumeRequestCache.size(),consumeRequestFlowControlTimes );

                    }
                    return;
                }

                long cachedMsgCount = processQueue.getMsgCount().get();
                long cachedMsgSizeInMb = processQueue.getMsgSize().get() / (1024 * 1024);

                if(cachedMsgCount > defaultLitePullConsumer.getPullThresholdForQueue()){
                    scheduledThreadPoolExecutor.schedule(this, PULL_TIME_DELAY_WHEN_FLOW_CONTROL, TimeUnit.MILLISECONDS);
                    if(queueFlowControlTimes++ % 1000 == 0){
                        log.warn("flow control");
                    }
                    return;
                }

                if(cachedMsgSizeInMb > defaultLitePullConsumer.getPullThresholdSizeForQueue()){
                    scheduledThreadPoolExecutor.schedule(this, PULL_TIME_DELAY_WHEN_FLOW_CONTROL, TimeUnit.MILLISECONDS);
                    if(queueFlowControlTimes++ % 1000 == 0){
                        log.warn("flow control");
                    }
                    return;
                }

                if(processQueue.getMaxSpan() > defaultLitePullConsumer.getConsumeMaxSpan()){
                    scheduledThreadPoolExecutor.schedule(this, PULL_TIME_DELAY_WHEN_FLOW_CONTROL, TimeUnit.MILLISECONDS);
                    if(queueMaxSpanFlowControlTimes++ % 1000 == 0){
                        log.warn("flow control");
                    }
                    return;
                }

                //拉取消息位点
                long offset = nextPullOffset(messageQueue);
                long pullDelayTimeMillis = 0;

                try{
                    SubscriptionData subscriptionData = null;
                    //获取订阅信息
                    String topic = messageQueue.getTopic();
                    if(SubscriptionType.SUBSCRIBE == DefaultLitePullConsumerImpl.this.subscribeType){
                        subscriptionData = rebalanceImpl.getSubscriptionInner().get(topic);
                    }else{
                        //build subscription data

                    }
                    PullResult pullResult = pull(messageQueue, subscriptionData, offset, defaultLitePullConsumer.getPullBatchSize());

                    switch (pullResult.getPullStatus()){
                        case FOUND:
                            final Object lockObj = DefaultLitePullConsumerImpl.this.messageQueueLock.fetchLockObject(messageQueue);
                            synchronized (lockObj){
                                if(pullResult.getMsgFoundList() != null && !pullResult.getMsgFoundList().isEmpty() && DefaultLitePullConsumerImpl.this.assignedMessageQueue.getSeekOffset(messageQueue) != -1){
                                    processQueue.putMessage(pullResult.getMsgFoundList());
                                    submitConsumeRequest(new ConsumeRequest(pullResult.getMsgFoundList(), messageQueue, processQueue));
                                }
                            }
                            break;
                        case OFFSET_ILLEGAL:
                            log.warn("the pull request offset illegal: {}", pullResult.toString());
                            break;
                        default:
                            break;
                    }
                    updatePullOffset(messageQueue, pullResult.getNextBeginOffset());
                }catch (Exception e){
                    pullDelayTimeMillis = pullDelayTimeMillisWhenException;
                }

                if(!this.isCancelled()){
                    scheduledThreadPoolExecutor.schedule(this, pullDelayTimeMillis, TimeUnit.MILLISECONDS);
                }else {
                    log.warn("The pull task is cancelled after doPullTask, messageQueue:{}", messageQueue);
                }
            }

        }

        public MessageQueue getMessageQueue() {
            return messageQueue;
        }

        public boolean isCancelled() {
            return cancelled;
        }

        public void setCancelled(boolean cancelled) {
            this.cancelled = cancelled;
        }
    }

    private void submitConsumeRequest(ConsumeRequest consumeRequest) {
        try{
            consumeRequestCache.put(consumeRequest);
        }catch (InterruptedException e){
            log.error("Submit consume request error", e);
        }
    }

    private void updatePullOffset(MessageQueue messageQueue, long nextBeginOffset) {
        if(assignedMessageQueue.getSeekOffset(messageQueue) != -1){
            assignedMessageQueue.updatePullOffset(messageQueue, nextBeginOffset);
        }
    }

    private long nextPullOffset(MessageQueue messageQueue) {
        long offset = -1;
        long seekOffset = assignedMessageQueue.getSeekOffset(messageQueue);
        //存在seekOffset则以seekOffset为准
        if(seekOffset != -1){
            offset = seekOffset;
            assignedMessageQueue.updateConsumeOffset(messageQueue,offset);
            assignedMessageQueue.setSeekOffset(messageQueue, -1);
        }else{
            offset = assignedMessageQueue.getPullOffset(messageQueue);
            if(offset == -1){
                offset = fetchConsumeOffset(messageQueue);
            }
        }
        return offset;
    }

    private long fetchConsumeOffset(MessageQueue messageQueue) {
        this.checkServiceState();
        return this.rebalanceImpl.computePullFromWhere(messageQueue);
    }


    private PullResult pull(MessageQueue messageQueue, SubscriptionData subscriptionData, long offset, int maxNums) throws MQClientException, MQBrokerException, RemotingException, InterruptedException {
        return pull(messageQueue, subscriptionData, offset, maxNums, defaultLitePullConsumer.getConsumerPullTimeoutMillis());
    }

    private PullResult pull(MessageQueue mq, SubscriptionData subscriptionData, long offset, int maxNums, long timeoutMills) throws MQClientException, MQBrokerException, RemotingException, InterruptedException {
        return pullSyncImpl(mq, subscriptionData, offset, maxNums, true, timeoutMills);
    }

    private PullResult pullSyncImpl(MessageQueue mq, SubscriptionData subscriptionData, long offset, int maxNums,
                                    boolean block, long timeout) throws MQClientException, MQBrokerException, RemotingException, InterruptedException {
        if(null == mq){
            throw new MQClientException("mq is null", null);
        }
        if(offset < 0){
            throw new MQClientException("offset less than zero", null);
        }
        if(maxNums <= 0){
            throw new MQClientException("max nums less or equal than zero", null);
        }
        int sysFlag = PullSysFlag.buildSysFlag(false, block, true, false, true);

        long timeoutMillis = block ? this.defaultLitePullConsumer.getConsumerTimeoutMillisWhenSuspend() : timeout;

        boolean isTagType = ExpressionType.isTagType(subscriptionData.getExpressionType());
        PullResult pullResult = this.pullAPIWrapper.pullKernelImpl(
                mq,
                subscriptionData.getSubString(),
                subscriptionData.getExpressionType(),
                isTagType ? 0L : subscriptionData.getSubVersion(),
                offset,
                maxNums,
                sysFlag,
                0,
                this.defaultLitePullConsumer.getBrokerSuspendMaxTimeMillis(),
                timeoutMillis,
                CommunicationMode.SYNC,
                null
        );
        this.pullAPIWrapper.processPullResult(mq, pullResult, subscriptionData);
        return pullResult;
    }

    public void checkServiceState(){

    }

    static class ConsumeRequest{
        private final List<MessageExt> messageExts;
        private final MessageQueue messageQueue;
        private final ProcessQueue processQueue;

        public ConsumeRequest(List<MessageExt> messageExts, MessageQueue messageQueue, ProcessQueue processQueue) {
            this.messageExts = messageExts;
            this.messageQueue = messageQueue;
            this.processQueue = processQueue;
        }

        public List<MessageExt> getMessageExts() {
            return messageExts;
        }

        public MessageQueue getMessageQueue() {
            return messageQueue;
        }

        public ProcessQueue getProcessQueue() {
            return processQueue;
        }
    }

}
