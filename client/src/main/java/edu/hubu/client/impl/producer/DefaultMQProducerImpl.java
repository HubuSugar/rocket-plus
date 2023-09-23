package edu.hubu.client.impl.producer;

import edu.hubu.client.exception.MQClientException;
import edu.hubu.client.impl.CommunicationMode;
import edu.hubu.client.impl.SendCallback;
import edu.hubu.client.impl.TopicPublishInfo;
import edu.hubu.client.instance.MQClientInstance;
import edu.hubu.client.instance.MQClientManager;
import edu.hubu.client.latency.MQFaultStrategy;
import edu.hubu.client.producer.DefaultMQProducer;
import edu.hubu.client.producer.MQProducerInner;
import edu.hubu.client.producer.SendResult;
import edu.hubu.client.producer.SendStatus;
import edu.hubu.remoting.netty.exception.RemotingException;
import edu.hubu.common.exception.broker.MQBrokerException;
import edu.hubu.common.message.Message;
import edu.hubu.common.message.MessageConst;
import edu.hubu.common.message.MessageDecoder;
import edu.hubu.common.message.MessageQueue;
import edu.hubu.common.protocol.SendMessageRequestHeader;
import edu.hubu.common.sysFlag.MessageSysFlag;
import lombok.extern.slf4j.Slf4j;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * @author: sugar
 * @date: 2023/5/25
 * @description:
 */
@Slf4j
public class DefaultMQProducerImpl implements MQProducerInner {

    private final DefaultMQProducer defaultMQProducer;
    //<topic, topicPublish>
    private final ConcurrentMap<String, TopicPublishInfo> topicPublishInfoTable = new ConcurrentHashMap<>();
    private MQClientInstance mqClientFactory;
    private final MQFaultStrategy mqFaultStrategy = new MQFaultStrategy();

    public DefaultMQProducerImpl(DefaultMQProducer defaultMQProducer) {
        this.defaultMQProducer = defaultMQProducer;

    }

    @Override
    public boolean isNeedUpdateTopicRoute(String topic) {
        TopicPublishInfo topicPublishInfo = topicPublishInfoTable.get(topic);
        return topicPublishInfo == null || !topicPublishInfo.ok();
    }

    @Override
    public void updateTopicPublishInfo(String topic, TopicPublishInfo publishInfo) {
        if (topic != null && publishInfo != null) {
            TopicPublishInfo prev = topicPublishInfoTable.put(topic, publishInfo);
            if (prev != null) {
                log.info("topic route data is not null, topic:{}, topic publish info:{}", topic, publishInfo);
            }
        }
    }

    @Override
    public Set<String> getPublishTopicList() {
        return new HashSet<>(topicPublishInfoTable.keySet());
    }

    public void start() throws MQClientException {
        start(true);
    }

    public void start(final boolean startFactory) throws MQClientException {
        //默认生成topic
        this.topicPublishInfoTable.put(defaultMQProducer.getCreateTopicKey(), new TopicPublishInfo());
        this.mqClientFactory = MQClientManager.getInstance().getOrCreateInstance(this.defaultMQProducer);
        boolean b = this.mqClientFactory.registerProducer(this.defaultMQProducer.getProducerGroup(), this);

        if(!b){
            throw new MQClientException(1,"register producer exception");
        }

        if(startFactory){
            mqClientFactory.start();
        }

    }

    public SendResult send(Message message) throws RemotingException, InterruptedException, MQClientException, MQBrokerException {
        return sendDefaultImpl(message, CommunicationMode.SYNC, null, this.defaultMQProducer.getSendMsgTimeout());
    }

    public void send(Message message, long timeout) throws RemotingException, InterruptedException, MQClientException, MQBrokerException {
        sendDefaultImpl(message, CommunicationMode.SYNC, null, timeout);
    }

    public void send(Message message, SendCallback sendCallback, long timeout) throws RemotingException, InterruptedException, MQClientException, MQBrokerException {
        sendDefaultImpl(message, CommunicationMode.ASYNC, sendCallback, timeout);
    }

    private SendResult sendDefaultImpl(Message message, CommunicationMode mode, SendCallback sendCallback, long timeout)
            throws MQBrokerException, RemotingException, InterruptedException, MQClientException {
        //1、tryToFindTopicPublishInfo(topic)
        TopicPublishInfo topicPublishInfo = this.tryToFindTopicPublishInfo(message.getTopic());

        if (topicPublishInfo.isHaveTopicRouteInfo() && topicPublishInfo.ok()) {
            //2、selectMessageQueue(topicPublishInfo, lastBrokerName)
            MessageQueue mq = null;
            for (int i = 0; i < 3; i++) {
                String lastBrokerName = mq == null ? null : mq.getBrokerName();
                MessageQueue mqSelected = this.selectOneMessageQueue(topicPublishInfo, lastBrokerName);
                if (mqSelected != null) {
                    mq = mqSelected;
                    //3、核心消息发送流程
                    SendResult sendResult = this.sendKernelImpl(message, mq, mode, sendCallback, topicPublishInfo, timeout);
                    switch (mode){
                        case ASYNC:
                        case ONEWAY:
                            return null;
                        case SYNC:
                            if(sendResult != null && sendResult.getSendStatus() != SendStatus.SEND_OK){
                                if(this.defaultMQProducer.isRetryAnotherBrokerWhenStoreFail()){
                                    continue;
                                }
                            }
                            return sendResult;
                    }
                    return sendResult;
                }
            }
        }

        throw new MQClientException(10005, "no route info for topic " + message.getTopic(), null);
    }

    /**
     * 查询topic信息
     *
     * @param topic
     * @return
     */
    private TopicPublishInfo tryToFindTopicPublishInfo(final String topic) {
        TopicPublishInfo topicPublishInfo = topicPublishInfoTable.get(topic);
        if (topicPublishInfo == null || !topicPublishInfo.ok()) {
            //更新nameSrv维护的topicRouteInfo
            this.topicPublishInfoTable.putIfAbsent(topic, new TopicPublishInfo());
            //直接从nameSrv获取topicRouteInfo
            this.mqClientFactory.updateTopicInfoFromNameServer(topic);
            topicPublishInfo = topicPublishInfoTable.get(topic);
        }
        //用本来的topic没有查询到对应的topic信息，通过系统内置的topic自动创建topic
        if (topicPublishInfo.isHaveTopicRouteInfo() || topicPublishInfo.ok()) {
            return topicPublishInfo;
        } else {
            this.mqClientFactory.updateTopicInfoFromNameServer(topic, true, this.defaultMQProducer);
            topicPublishInfo = this.topicPublishInfoTable.get(topic);
            return topicPublishInfo;
        }
    }

    /**
     * 选择待发送的消息队列
     *
     * @param topicPublishInfo
     * @param lastBrokerName
     * @return
     */
    private MessageQueue selectOneMessageQueue(TopicPublishInfo topicPublishInfo, final String lastBrokerName) {
        return mqFaultStrategy.selectOneMessageQueue(topicPublishInfo, lastBrokerName);
    }


    private SendResult sendKernelImpl(Message message, MessageQueue mq, CommunicationMode mode, SendCallback sendCallback, TopicPublishInfo topicPublishInfo, long timeout)
            throws RemotingException, InterruptedException, MQClientException, MQBrokerException {
        long beginTime = System.currentTimeMillis();
        //1、findBrokerAddress
        String brokerAddr = mqClientFactory.findBrokerAddressInPublish(mq.getBrokerName());

        if(brokerAddr == null){
            tryToFindTopicPublishInfo(message.getTopic());
            brokerAddr = mqClientFactory.findBrokerAddressInPublish(mq.getBrokerName());
        }

        if(brokerAddr != null){

            byte[] prevBody = message.getBody();
            try{
                int sysFlag = 0;
                //判断是不是压缩消息

                //判断是不是半消息Prepare_message
                String property = message.getProperty(MessageConst.PROPERTY_TRANSACTION_PREPARE);
                if(Boolean.parseBoolean(property)){
                    sysFlag |= MessageSysFlag.TRANSACTION_PREPARE_TYPE;
                }

                //2、buildCustomHeader
                SendMessageRequestHeader requestHeader = new SendMessageRequestHeader();
                requestHeader.setProducerGroup(defaultMQProducer.getProducerGroup());
                requestHeader.setDefaultTopic(defaultMQProducer.getCreateTopicKey());
                requestHeader.setDefaultTopicQueueNums(defaultMQProducer.getDefaultQueueNums());
                requestHeader.setQueueId(mq.getQueueId());
                requestHeader.setTopic(message.getTopic());
                requestHeader.setBornTimestamp(System.currentTimeMillis());
                requestHeader.setFlag(message.getFlag());
                requestHeader.setProperties(MessageDecoder.properties2String(message.getProperties()));
                requestHeader.setReconsumeTimes(0);
                requestHeader.setSysFlag(sysFlag);


                SendResult sendResult = null;
                switch (mode){
                    case ASYNC:
                        break;
                    case SYNC:
                        long costTimeSync = System.currentTimeMillis() - beginTime;

                        //3、invokeRemoting
                        sendResult = mqClientFactory.getMqClientAPI().sendMessage(brokerAddr,
                                mq.getBrokerName(),
                                message,
                                requestHeader,
                                timeout - costTimeSync,
                                mode,
                                null,
                                this);
                        break;
                }
                return sendResult;
            }catch (RemotingException e){
                //execute hook
                log.warn("send message remoting exception", e);
                throw e;
            }catch (MQBrokerException e){
                log.warn("send message mq broker exception", e);
                throw e;
            }catch (InterruptedException e){
                log.warn("send message interrupted exception", e);
                throw e;
            }finally {
                message.setBody(prevBody);
                message.setTopic(message.getTopic());
            }
        }

        throw new MQClientException("the broker [" +mq.getBrokerName()+ "] does not exist", null);
    }



}
