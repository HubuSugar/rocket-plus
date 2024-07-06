package edu.hubu.client.consumer;

import edu.hubu.client.consumer.store.OffsetStore;
import edu.hubu.client.consumer.strategy.AllocateMessageQueueAveragely;
import edu.hubu.client.consumer.strategy.AllocateMessageQueueStrategy;
import edu.hubu.client.exception.MQClientException;
import edu.hubu.client.impl.consumer.DefaultLitePullConsumerImpl;
import edu.hubu.client.instance.ClientConfig;
import edu.hubu.common.consumer.ConsumeFromWhere;
import edu.hubu.common.message.MessageExt;
import edu.hubu.common.protocol.heartbeat.MessageModel;
import edu.hubu.common.topic.NamespaceUtil;
import edu.hubu.remoting.netty.handler.RpcHook;

import java.util.List;

/**
 * @author: sugar
 * @date: 2023/10/28
 * @description:
 */
public class DefaultLitePullConsumer extends ClientConfig implements LitePullConsumer {

    private final DefaultLitePullConsumerImpl defaultLitePullConsumerImpl;

    private String consumerGroup;

    private long brokerSuspendMaxTimeMillis = 1000 * 20;

    private long consumerTimeoutMillisWhenSuspend = 1000 * 30;

    private MessageModel messageModel = MessageModel.CLUSTERING;

    private MessageQueueListener messageQueueListener;

    private OffsetStore offsetStore;

    //默认采用平均分配算法
    private AllocateMessageQueueStrategy allocateMessageQueueStrategy = new AllocateMessageQueueAveragely();

    /**
     * 是否以订阅组为单位
     */
    private boolean unitMode = false;
    private boolean autoCommit = true;

    private int pullThreadNums = 20;
    private int pullBatchSize = 10;
    private int pullThresholdForAll = 10000;
    private int pullThresholdForQueue = 1000;
    private int pullThresholdSizeForQueue = 100;
    private int consumeMaxSpan = 200;


    private ConsumeFromWhere consumeFromWhere = ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET;
    // the socket timeout
    private long consumerPullTimeoutMillis = 1000 * 10;
    private long autoCommitIntervalMillis = 5 * 1000;
    private long topicMetadataCheckIntervalMillis = 1000 * 30;


    public DefaultLitePullConsumer(final String consumerGroup) {
        this(null, consumerGroup, null);
    }

    public DefaultLitePullConsumer(final String namespace, final String consumerGroup, final RpcHook rpcHook) {
        this.namespace = namespace;
        this.consumerGroup = consumerGroup;
        this.defaultLitePullConsumerImpl = new DefaultLitePullConsumerImpl(this, rpcHook);
    }

    @Override
    public void start() throws MQClientException {
        setConsumerGroup(NamespaceUtil.wrapNamespace(this.namespace, this.consumerGroup));
        this.defaultLitePullConsumerImpl.start();
    }

    @Override
    public void shutdown() {

    }


    @Override
    public void subscribe(String topic, String subExpression) throws MQClientException {
        this.defaultLitePullConsumerImpl.subscribe(NamespaceUtil.wrapNamespace(this.namespace, topic), subExpression);
    }

    @Override
    public void subscribe(String topic, MessageSelector messageSelector) {
        this.defaultLitePullConsumerImpl.subscribe(NamespaceUtil.wrapNamespace(this.namespace, topic), messageSelector);
    }

    @Override
    public List<MessageExt> poll() {
        return this.defaultLitePullConsumerImpl.poll(this.getConsumerPullTimeoutMillis());
    }

    @Override
    public List<MessageExt> poll(long timeoutMillis) {
        return this.defaultLitePullConsumerImpl.poll(timeoutMillis);
    }

    @Override
    public boolean isAutoCommit() {
        return autoCommit;
    }

    public void setAutoCommit(boolean autoCommit) {
        this.autoCommit = autoCommit;
    }

    @Override
    public boolean isUnitMode() {
        return this.unitMode;
    }

    @Override
    public void setUnitMode(boolean unitMode) {
        this.unitMode = unitMode;
    }

    public DefaultLitePullConsumerImpl getDefaultLitePullConsumerImpl() {
        return defaultLitePullConsumerImpl;
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

    public long getBrokerSuspendMaxTimeMillis() {
        return brokerSuspendMaxTimeMillis;
    }

    public void setBrokerSuspendMaxTimeMillis(long brokerSuspendMaxTimeMillis) {
        this.brokerSuspendMaxTimeMillis = brokerSuspendMaxTimeMillis;
    }

    public long getConsumerTimeoutMillisWhenSuspend() {
        return consumerTimeoutMillisWhenSuspend;
    }

    public void setConsumerTimeoutMillisWhenSuspend(long consumerTimeoutMillisWhenSuspend) {
        this.consumerTimeoutMillisWhenSuspend = consumerTimeoutMillisWhenSuspend;
    }

    public void setMessageModel(MessageModel messageModel) {
        this.messageModel = messageModel;
    }

    public MessageQueueListener getMessageQueueListener() {
        return messageQueueListener;
    }

    public void setMessageQueueListener(MessageQueueListener messageQueueListener) {
        this.messageQueueListener = messageQueueListener;
    }

    public ConsumeFromWhere getConsumeFromWhere() {
        return consumeFromWhere;
    }

    public void setConsumeFromWhere(ConsumeFromWhere consumeFromWhere) {
        this.consumeFromWhere = consumeFromWhere;
    }

    public OffsetStore getOffsetStore() {
        return offsetStore;
    }

    public void setOffsetStore(OffsetStore offsetStore) {
        this.offsetStore = offsetStore;
    }

    public AllocateMessageQueueStrategy getAllocateMessageQueueStrategy() {
        return allocateMessageQueueStrategy;
    }

    public void setAllocateMessageQueueStrategy(AllocateMessageQueueStrategy allocateMessageQueueStrategy) {
        this.allocateMessageQueueStrategy = allocateMessageQueueStrategy;
    }

    public int getPullThreadNums() {
        return pullThreadNums;
    }

    public void setPullThreadNums(int pullThreadNums) {
        this.pullThreadNums = pullThreadNums;
    }

    public int getPullBatchSize() {
        return pullBatchSize;
    }

    public void setPullBatchSize(int pullBatchSize) {
        this.pullBatchSize = pullBatchSize;
    }

    public int getPullThresholdForAll() {
        return pullThresholdForAll;
    }

    public void setPullThresholdForAll(int pullThresholdForAll) {
        this.pullThresholdForAll = pullThresholdForAll;
    }

    public int getPullThresholdForQueue() {
        return pullThresholdForQueue;
    }

    public void setPullThresholdForQueue(int pullThresholdForQueue) {
        this.pullThresholdForQueue = pullThresholdForQueue;
    }

    public int getPullThresholdSizeForQueue() {
        return pullThresholdSizeForQueue;
    }

    public void setPullThresholdSizeForQueue(int pullThresholdSizeForQueue) {
        this.pullThresholdSizeForQueue = pullThresholdSizeForQueue;
    }

    public int getConsumeMaxSpan() {
        return consumeMaxSpan;
    }

    public void setConsumeMaxSpan(int consumeMaxSpan) {
        this.consumeMaxSpan = consumeMaxSpan;
    }

    public long getConsumerPullTimeoutMillis() {
        return consumerPullTimeoutMillis;
    }

    public void setConsumerPullTimeoutMillis(long consumerPullTimeoutMillis) {
        this.consumerPullTimeoutMillis = consumerPullTimeoutMillis;
    }

    public long getAutoCommitIntervalMillis() {
        return autoCommitIntervalMillis;
    }

    public void setAutoCommitIntervalMillis(long autoCommitIntervalMillis) {
        this.autoCommitIntervalMillis = autoCommitIntervalMillis;
    }

    public long getTopicMetadataCheckIntervalMillis() {
        return topicMetadataCheckIntervalMillis;
    }

    public void setTopicMetadataCheckIntervalMillis(long topicMetadataCheckIntervalMillis) {
        this.topicMetadataCheckIntervalMillis = topicMetadataCheckIntervalMillis;
    }
}
