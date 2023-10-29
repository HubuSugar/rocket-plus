package edu.hubu.client.consumer;

import edu.hubu.client.consumer.store.OffsetStore;
import edu.hubu.client.consumer.strategy.AllocateMessageQueueAveragely;
import edu.hubu.client.consumer.strategy.AllocateMessageQueueStrategy;
import edu.hubu.client.exception.MQClientException;
import edu.hubu.client.impl.consumer.DefaultLitePullConsumerImpl;
import edu.hubu.client.instance.ClientConfig;
import edu.hubu.common.protocol.heartbeat.MessageModel;
import edu.hubu.common.topic.NamespaceUtil;
import edu.hubu.remoting.netty.handler.RpcHook;

/**
 * @author: sugar
 * @date: 2023/10/28
 * @description:
 */
public class DefaultLitePullConsumer extends ClientConfig implements LitePullConsumer {

    private final DefaultLitePullConsumerImpl defaultLitePullConsumerImpl;

    private String consumerGroup;

    private MessageModel messageModel = MessageModel.CLUSTERING;

    private OffsetStore offsetStore;

    private AllocateMessageQueueStrategy allocateMessageQueueStrategy = new AllocateMessageQueueAveragely();

    private boolean unitMode = false;


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
    public void subscribe(String topic, String subExpression) {
        this.defaultLitePullConsumerImpl.subscribe(NamespaceUtil.wrapNamespace(this.namespace, topic), subExpression);
    }

    @Override
    public void subscribe(String topic, MessageSelector messageSelector) {
        this.defaultLitePullConsumerImpl.subscribe(NamespaceUtil.wrapNamespace(this.namespace, topic), messageSelector);
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

    public void setMessageModel(MessageModel messageModel) {
        this.messageModel = messageModel;
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
}
