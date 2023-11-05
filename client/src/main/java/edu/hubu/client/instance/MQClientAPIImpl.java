package edu.hubu.client.instance;

import edu.hubu.client.exception.MQClientException;
import edu.hubu.client.hook.SendMessageContext;
import edu.hubu.client.impl.CommunicationMode;
import edu.hubu.client.impl.SendCallback;
import edu.hubu.client.impl.TopicPublishInfo;
import edu.hubu.client.impl.producer.DefaultMQProducerImpl;
import edu.hubu.client.producer.SendResult;
import edu.hubu.client.producer.SendStatus;
import edu.hubu.common.exception.broker.MQBrokerException;
import edu.hubu.common.message.*;
import edu.hubu.common.protocol.SendMessageRequestHeader;
import edu.hubu.common.protocol.body.LockBatchRequestBody;
import edu.hubu.common.protocol.body.LockBatchResponseBody;
import edu.hubu.common.protocol.header.request.GetConsumerIdListByGroupRequestHeader;
import edu.hubu.common.protocol.header.request.GetMaxOffsetRequestHeader;
import edu.hubu.common.protocol.header.request.GetTopicRouteInfoHeader;
import edu.hubu.common.protocol.body.GetConsumerIdListByGroupResponseBody;
import edu.hubu.common.protocol.header.response.GetMaxOffsetResponseHeader;
import edu.hubu.common.protocol.header.response.SendMessageResponseHeader;
import edu.hubu.common.protocol.request.RequestCode;
import edu.hubu.common.protocol.route.TopicRouteData;
import edu.hubu.common.utils.MixAll;
import edu.hubu.remoting.netty.*;
import edu.hubu.remoting.netty.exception.*;
import io.netty.util.internal.StringUtil;
import lombok.extern.slf4j.Slf4j;

import java.util.Arrays;
import java.util.List;
import java.util.Set;

/**
 * @author: sugar
 * @date: 2023/5/26
 * @description:
 */
@Slf4j
public class MQClientAPIImpl {

    private final RemotingClient remotingClient;
    private final NettyClientConfig nettyClientConfig;
    private String nameServer;
    //获取namespace
    private final ClientConfig clientConfig;

    public MQClientAPIImpl(NettyClientConfig nettyClientConfig, ClientConfig clientConfig) {
        this.nettyClientConfig = nettyClientConfig;
        this.remotingClient = new NettyRemotingClient(nettyClientConfig);
        this.clientConfig = clientConfig;
    }

    public void start(){
        this.remotingClient.start();
    }

    //更新nameSrv地址
    public void updateNameSrvAddressList(String nameServer) {
        String[] address = nameServer.split(";");
        List<String> strings = Arrays.asList(address);
        this.remotingClient.updateNameSrvAddress(strings);
    }

    /**
     *
     */
    public SendResult sendMessage(String brokerAddress, String brokerName, final Message message, final SendMessageRequestHeader requestHeader,
                                  long timeout, CommunicationMode mode, SendMessageContext context, DefaultMQProducerImpl producer)
            throws RemotingException, InterruptedException, MQBrokerException {
        return sendMessage(brokerAddress,brokerName,message, requestHeader, timeout, mode, null, null, null, 0, context, producer);
    }

    public SendResult sendMessage(
            final String brokerAddr,
            final String brokerName,
            final Message message,
            final SendMessageRequestHeader requestHeader,
            final long timeout,
            final CommunicationMode mode,
            final SendCallback sendCallback,
            final TopicPublishInfo topicPublishInfo,
            final MQClientInstance instance,
            final int retryTimesWhenFailed,
            final SendMessageContext context,
            final DefaultMQProducerImpl producer
            ) throws RemotingException, InterruptedException, MQBrokerException {

        long beginTime = System.currentTimeMillis();
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.SEND_MESSAGE, requestHeader);
        request.setBody(message.getBody());
        switch (mode){
            case ONEWAY:
            case ASYNC:

                return null;
            case SYNC:
                long costTimeSync = System.currentTimeMillis() - beginTime;
                if(timeout < costTimeSync){
                    throw new RemotingTooMuchRequestException("call time out");
                }
                return sendMessageSync(brokerAddr, brokerName, message, request, timeout);
        }

        return null;
    }

    private SendResult sendMessageSync(String brokerAddr, String brokerName, Message message, RemotingCommand request, long timeout) throws RemotingConnectException,
            RemotingSendRequestException, RemotingTimeoutException, InterruptedException, MQBrokerException {

        RemotingCommand response = remotingClient.invokeSync(brokerAddr, request, timeout);
        assert response != null;

        return this.processResponse(brokerName, message, response);
    }

    private SendResult processResponse(String brokerName, Message message, RemotingCommand response) throws MQBrokerException {
        SendStatus sendStatus;
        switch (response.getCode()){
            case ResponseCode.FLUSH_DISK_TIMEOUT:
                sendStatus = SendStatus.FLUSH_DISK_TIMEOUT;
                break;
            case ResponseCode.SUCCESS:
                sendStatus = SendStatus.SEND_OK;
                break;
            case ResponseCode.FLUSH_SLAVE_TIMEOUT:
                sendStatus = SendStatus.FLUSH_SLAVE_TIMEOUT;
                break;
            case ResponseCode.SLAVE_NOT_AVAILABLE:
                sendStatus = SendStatus.SLAVE_NOT_AVAILABLE;
                break;
            default:
                throw new MQBrokerException(response.getCode(), response.getRemark());
        }

        SendMessageResponseHeader responseHeader = (SendMessageResponseHeader) response.decodeCustomCommandHeader(SendMessageResponseHeader.class);

        //reset topic without namespace
        String topic = message.getTopic();
        if(!StringUtil.isNullOrEmpty(clientConfig.getNamespace())){
             //todo
            topic = "withoutNamespace";
        }

        //处理messageQueue
        MessageQueue messageQueue = new MessageQueue(topic, brokerName, responseHeader.getQueueId());

        //批量消息全局消息id
        String uniqMsgId = MessageClientIDSetter.getUniqID(message);
        if(message instanceof MessageBatch){
            StringBuilder sb = new StringBuilder();
            for (Message msg : (MessageBatch) message) {
                sb.append((sb.length() == 0 ? "": ",")).append(MessageClientIDSetter.getUniqID(msg));
            }
            uniqMsgId = sb.toString();
        }
        SendResult sendResult = new SendResult();
        sendResult.setSendStatus(sendStatus);
        sendResult.setMessageQueue(messageQueue);
        sendResult.setMsgId(uniqMsgId);
        sendResult.setOffsetMsgId(responseHeader.getMsgId());
        sendResult.setQueueOffset(responseHeader.getQueueOffset());

        //设置事务消息id
        sendResult.setTransactionId(responseHeader.getTransactionId());
        //设置regionId和trace
        String regionId = response.getExtFields().get(MessageConst.PROPERTY_REGION_ID);
        String traceOn = response.getExtFields().get(MessageConst.PROPERTY_TRACE);
        if(StringUtil.isNullOrEmpty(regionId)){
            regionId = MixAll.DEFAULT_TRACE_REGION;
        }
        sendResult.setRegionId(regionId);
        sendResult.setTraceOn(traceOn == null || "true".equalsIgnoreCase(traceOn));

        return sendResult;
    }


    /**
     * 通过默认topic从nameServer获取topicRouteInfo
     * 通过RPC从nameSrv获取， DefaultProcessor
     * @param createTopicKey
     * @param timeout
     */
    public TopicRouteData getDefaultTopicRouteFromNameSrv(String createTopicKey, long timeout) throws RemotingException, InterruptedException, MQClientException {
        return getTopicRouteFromNameSrv(createTopicKey, timeout, false);
    }

    public TopicRouteData getTopicRouteFromNameSrv(String topic, long timeout) throws RemotingException, InterruptedException, MQClientException {
        return getTopicRouteFromNameSrv(topic, timeout, true);
    }

    public TopicRouteData getTopicRouteFromNameSrv(String topic, long timeout, boolean allowTopicAbsent) throws RemotingException, MQClientException, InterruptedException {
        GetTopicRouteInfoHeader topicRouteInfoHeader = new GetTopicRouteInfoHeader();
        topicRouteInfoHeader.setTopic(topic);
        RemotingCommand requestCommand = RemotingCommand.createRequestCommand(RequestCode.GET_ROUTE_INFO_BY_TOPIC, topicRouteInfoHeader);
        RemotingCommand response = remotingClient.invokeSync(null, requestCommand, timeout);
        assert response != null;

        int code = response.getCode();
        switch (code) {
            case ResponseCode.TOPIC_NOT_EXIST:
                if (allowTopicAbsent) {
                    log.warn("topic:{} not exist", topic);
                }
                break;
            case ResponseCode.SUCCESS:
                if (response.getBody() != null) {
                    return TopicRouteData.decode(response.getBody(), TopicRouteData.class);
                }
                break;
            default:
                break;
        }
        throw new MQClientException(response.getCode(), response.getRemark());
    }

    /**
     * 通过RPC请求获取消费者集合
     * @param brokerAddr
     * @param consumerGroup
     * @param timeoutMillis
     * @return
     */
    public List<String> getConsumerIdListByGroup(String brokerAddr, String consumerGroup, long timeoutMillis)
            throws RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException, MQBrokerException {
        GetConsumerIdListByGroupRequestHeader requestHeader = new GetConsumerIdListByGroupRequestHeader();
        requestHeader.setConsumerGroup(consumerGroup);

        RemotingCommand requestCommand = RemotingCommand.createRequestCommand(RequestCode.GET_CONSUMER_LIST_BY_GROUP, requestHeader);
        RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVipChannel(this.clientConfig.isVipChannelEnable(), brokerAddr), requestCommand, timeoutMillis);
        assert response != null;
        switch (response.getCode()){
            case ResponseCode.SUCCESS:
                if(response.getBody() != null){
                    GetConsumerIdListByGroupResponseBody groupResponse = GetConsumerIdListByGroupResponseBody.decode(response.getBody(), GetConsumerIdListByGroupResponseBody.class);
                    return groupResponse.getConsumerIdList();
                }
                break;
            default:
                break;
        }
        throw new MQBrokerException(response.getCode(), response.getRemark());
    }

    /**
     * 批量锁定mq
     * @param brokerAddr
     * @param requestBody 客户端id、consumerGroup
     * @param timeoutMillis
     * @return
     */
    public Set<MessageQueue> lockBatchMQ(String brokerAddr, LockBatchRequestBody requestBody, long timeoutMillis)
            throws RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException, MQBrokerException {

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.LOCK_BATCH_MQ, null);
        request.setBody(requestBody.encode());

        RemotingCommand response = this.remotingClient.invokeSync(brokerAddr, request, timeoutMillis);
        switch (response.getCode()){
            case ResponseCode.SUCCESS:
                LockBatchResponseBody responseBody = LockBatchResponseBody.decode(response.getBody(), LockBatchResponseBody.class);
                return responseBody.getLockedMessageQueue();
            default:
                break;
        }

        throw new MQBrokerException(response.getCode(), response.getRemark());
    }

    public long getMaxOffset(String brokerAddr, String topic, int queueId, long timeoutMillis) throws RemotingConnectException, RemotingSendRequestException,
            RemotingTimeoutException, InterruptedException, MQBrokerException {
        GetMaxOffsetRequestHeader requestHeader = new GetMaxOffsetRequestHeader();
        requestHeader.setTopic(topic);
        requestHeader.setQueueId(queueId);
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_MAX_OFFSET, requestHeader);

        RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVipChannel(this.clientConfig.isVipChannelEnable(), brokerAddr), request, timeoutMillis);
        assert response != null;
        switch (response.getCode()){
            case ResponseCode.SUCCESS:
               GetMaxOffsetResponseHeader responseHeader = (GetMaxOffsetResponseHeader) response.decodeCustomCommandHeader(GetMaxOffsetResponseHeader.class);
               return responseHeader.getOffset();
            default:
                break;
        }

        throw new MQBrokerException(response.getCode(), response.getRemark());
    }

    public String fetchNameSrvAddress(){
        return nameServer;
    }

    public String getNameServer() {
        return nameServer;
    }

    public void setNameServer(String nameServer) {
        this.nameServer = nameServer;
    }

}
