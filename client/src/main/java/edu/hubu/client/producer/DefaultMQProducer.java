package edu.hubu.client.producer;

import edu.hubu.client.exception.MQClientException;
import edu.hubu.client.impl.producer.DefaultMQProducerImpl;
import edu.hubu.client.instance.ClientConfig;
import edu.hubu.remoting.netty.exception.RemotingException;
import edu.hubu.common.exception.broker.MQBrokerException;
import edu.hubu.common.message.Message;
import edu.hubu.common.topic.TopicValidator;

/**
 * @author: sugar
 * @date: 2023/5/25
 * @description:
 */
public class DefaultMQProducer extends ClientConfig {
    private long sendMsgTimeout = 5000;
    private String createTopicKey = TopicValidator.AUTO_CREATE_TOPIC_KEY_TOPIC;
    private int defaultQueueNums = 8;
    private final DefaultMQProducerImpl defaultMQProducerImpl;
    private String producerGroup;
    private boolean retryAnotherBrokerWhenStoreFail = false;

    public DefaultMQProducer(String producerGroup) {
        this.producerGroup = producerGroup;
        this.defaultMQProducerImpl = new DefaultMQProducerImpl(this);
    }

    public void start() throws MQClientException {

        this.defaultMQProducerImpl.start();
    }

    public void send(Message message, long timeout) throws RemotingException, InterruptedException, MQClientException, MQBrokerException {
        this.defaultMQProducerImpl.send(message, timeout);
    }

    public SendResult send(Message message) throws RemotingException, InterruptedException, MQClientException, MQBrokerException {
        return this.defaultMQProducerImpl.send(message);
    }

    public long getSendMsgTimeout() {
        return sendMsgTimeout;
    }

    public void setSendMsgTimeout(long sendMsgTimeout) {
        this.sendMsgTimeout = sendMsgTimeout;
    }

    public String getCreateTopicKey() {
        return createTopicKey;
    }

    public void setCreateTopicKey(String createTopicKey) {
        this.createTopicKey = createTopicKey;
    }

    public int getDefaultQueueNums() {
        return defaultQueueNums;
    }

    public void setDefaultQueueNums(int defaultQueueNums) {
        this.defaultQueueNums = defaultQueueNums;
    }

    public String getProducerGroup() {
        return producerGroup;
    }

    public void setProducerGroup(String producerGroup) {
        this.producerGroup = producerGroup;
    }

    public boolean isRetryAnotherBrokerWhenStoreFail() {
        return retryAnotherBrokerWhenStoreFail;
    }

    public void setRetryAnotherBrokerWhenStoreFail(boolean retryAnotherBrokerWhenStoreFail) {
        this.retryAnotherBrokerWhenStoreFail = retryAnotherBrokerWhenStoreFail;
    }
}
