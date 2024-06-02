package edu.hubu.broker.processor;

import edu.hubu.broker.client.ConsumerGroupInfo;
import edu.hubu.broker.filter.ConsumerFilterData;
import edu.hubu.broker.filter.ConsumerFilterManager;
import edu.hubu.broker.filter.ExpressionForRetryMessageFilter;
import edu.hubu.broker.filter.ExpressionMessageFilter;
import edu.hubu.broker.longpolling.PullRequest;
import edu.hubu.broker.pageCache.ManyMessageTransfer;
import edu.hubu.broker.starter.BrokerController;
import edu.hubu.common.PermName;
import edu.hubu.common.TopicConfig;
import edu.hubu.common.filter.ExpressionType;
import edu.hubu.common.filter.FilterAPI;
import edu.hubu.common.message.*;
import edu.hubu.common.protocol.header.request.PullMessageRequestHeader;
import edu.hubu.common.protocol.header.response.PullMessageResponseHeader;
import edu.hubu.common.protocol.heartbeat.MessageModel;
import edu.hubu.common.protocol.heartbeat.SubscriptionData;
import edu.hubu.common.protocol.topic.OffsetMovedEvent;
import edu.hubu.common.subcription.SubscriptionGroupConfig;
import edu.hubu.common.sysFlag.MessageSysFlag;
import edu.hubu.common.sysFlag.PullSysFlag;
import edu.hubu.common.topic.TopicValidator;
import edu.hubu.common.utils.MixAll;
import edu.hubu.remoting.netty.CustomCommandHeader;
import edu.hubu.remoting.netty.RemotingCommand;
import edu.hubu.remoting.netty.RemotingHelper;
import edu.hubu.remoting.netty.ResponseCode;
import edu.hubu.remoting.netty.handler.AsyncNettyRequestProcessor;
import edu.hubu.remoting.netty.handler.NettyRequestProcessor;
import edu.hubu.remoting.netty.handler.ResponseInvokeCallback;
import edu.hubu.store.GetMessageResult;
import edu.hubu.store.MessageFilter;
import edu.hubu.store.config.BrokerRole;
import io.netty.channel.*;
import lombok.extern.slf4j.Slf4j;
import org.ietf.jgss.MessageProp;

import javax.print.DocFlavor;
import java.nio.ByteBuffer;
import java.util.List;

/**
 * @author: sugar
 * @date: 2023/11/1
 * @description:
 */
@Slf4j
public class PullMessageProcessor extends AsyncNettyRequestProcessor implements NettyRequestProcessor {

    private final BrokerController brokerController;

    public PullMessageProcessor(BrokerController brokerController) {
        this.brokerController = brokerController;
    }

    @Override
    public RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand request) throws Exception {
        return this.processRequest(ctx.channel(), request, true);
    }


    @Override
    public boolean rejectRequest() {
        return false;
    }

    private RemotingCommand processRequest(final Channel channel, final RemotingCommand request, final boolean brokerAllowSuspend) {
        RemotingCommand response = RemotingCommand.createResponseCommand(PullMessageResponseHeader.class);
        final PullMessageResponseHeader responseHeader = (PullMessageResponseHeader) response.readCustomHeader();
        final PullMessageRequestHeader requestHeader = (PullMessageRequestHeader) request.decodeCustomCommandHeader(PullMessageRequestHeader.class);

        response.setOpaque(request.getOpaque());

        log.debug("process pull message request:{}", request);
        //判断broker的权限
        if (!PermName.isReadable(this.brokerController.getBrokerConfig().getBrokerPermission())) {
            response.setCode(ResponseCode.NO_PERMISSION);
            response.setRemark(String.format("the broker [%s] pulling message is forbidden", this.brokerController.getBrokerConfig().getBrokerName()));
            return response;
        }

        SubscriptionGroupConfig subscriptionGroupConfig =
                this.brokerController.getSubscriptionGroupManager().findSubscriptionGroupConfig(requestHeader.getConsumerGroup());
        //不存在订阅关系
        if (null == subscriptionGroupConfig) {
            response.setCode(ResponseCode.SUBSCRIPTION_GROUP_NOT_EXISTS);
            response.setRemark(String.format("subscription group [%s] does not exist, %s", requestHeader.getConsumerGroup(), "https://www.rocketmq.com"));
            return response;
        }

        //不能消费
        if (!subscriptionGroupConfig.isConsumeEnable()) {
            response.setCode(ResponseCode.NO_PERMISSION);
            response.setRemark("subscription group has no permission " + requestHeader.getConsumerGroup());
            return response;
        }

        //判断是否需要长轮询
        final boolean hasSuspendFlag = PullSysFlag.hasSuspendFlag(requestHeader.getSysFlag());
        //
        final boolean hasCommitlogFlag = PullSysFlag.hasCommitLogFlag(requestHeader.getSysFlag());
        //是否需要消息过滤
        final boolean hasSubscriptionFlag = PullSysFlag.hasSubscriptionFlag(requestHeader.getSysFlag());

        //请求挂起的时间
        final long suspendTimeoutMillis = hasSuspendFlag ? requestHeader.getSuspendTimeoutMillis() : 0;

        TopicConfig topicConfig = this.brokerController.getTopicConfigManager().selectTopicConfig(requestHeader.getTopic());
        if (topicConfig == null) {
            log.error("the topic {} not exist, consumer: {}", requestHeader.getTopic(), requestHeader.getConsumerGroup());
            response.setCode(ResponseCode.TOPIC_NOT_EXIST);
            response.setRemark(String.format("topic [%s] does not exist, apply first please", requestHeader.getTopic()));
            return response;
        }

        if (!PermName.isReadable(topicConfig.getPerm())) {
            response.setCode(ResponseCode.NO_PERMISSION);
            response.setRemark(String.format("the topic [%s] pull message is forbidden", requestHeader.getTopic()));
            return response;
        }

        if( requestHeader.getQueueId() < 0 || requestHeader.getQueueId() >= topicConfig.getReadQueueNums()){
            String errInfo = String.format("queueId is illegal, topic: [%s] topicConfig.readQueueNums: [%d] consumer: [%s]", requestHeader.getTopic(), topicConfig.getReadQueueNums(),
                    requestHeader.getConsumerGroup());
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark(errInfo);
            return response;
        }

        SubscriptionData subscriptionData = null;
        ConsumerFilterData consumerFilterData = null;

        if (hasSubscriptionFlag) {
            try{
                subscriptionData = FilterAPI.build(requestHeader.getTopic(), requestHeader.getSubscription(), requestHeader.getExpressionType());
                if(!ExpressionType.isTagType(subscriptionData.getExpressionType())){
                    consumerFilterData = ConsumerFilterManager.build(
                            requestHeader.getTopic(), requestHeader.getConsumerGroup(), requestHeader.getSubscription(),
                            requestHeader.getExpressionType(),
                            requestHeader.getSubVersion()
                    );
                    assert consumerFilterData != null;
                }
            }catch (Exception e){
                log.error("Parse the consumer`s subscription [{}] failed, group:{}", requestHeader.getSubscription(), requestHeader.getConsumerGroup());
                response.setCode(ResponseCode.SUBSCRIPTION_PARSE_FAILED);
                response.setRemark("parse the consumer`s subscription failed");
                return response;
            }
        } else {
            ConsumerGroupInfo consumerGroupInfo = this.brokerController.getConsumerManager().getConsumerGroupInfo(requestHeader.getConsumerGroup());
            if (consumerGroupInfo == null) {
                log.warn("the consumer`s group info does not exist, {}", requestHeader.getConsumerGroup());
                response.setCode(ResponseCode.SUBSCRIPTION_NOT_EXISTS);
                response.setRemark(String.format("the consumer group info %s does not exist", requestHeader.getConsumerGroup()));
                return response;
            }

            if (!subscriptionGroupConfig.isConsumeBroadcastEnable() && consumerGroupInfo.getMessageModel() == MessageModel.BROADCASTING) {
                response.setCode(ResponseCode.NO_PERMISSION);
                response.setRemark(String.format("the consumer group [%s] can not consume by broadcast way", requestHeader.getConsumerGroup()));
                return response;
            }

            subscriptionData = consumerGroupInfo.findSubscriptionData(requestHeader.getTopic());

            //是不是最新的订阅关系
            if (subscriptionData.getSubVersion() < requestHeader.getSubVersion()) {
                log.warn("the subscription is not latest, consumer group:{}, {}", requestHeader.getConsumerGroup(), subscriptionData.getSubString());
                response.setCode(ResponseCode.SUBSCRIPTION_NOT_LATEST);
                response.setRemark("the consumer's subscription is not latest");
                return response;
            }
        }

        if (!ExpressionType.isTagType(subscriptionData.getExpressionType())
            && !this.brokerController.getBrokerConfig().isEnablePropertyFilter()) {
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark("The broker does support consumer filter message by " + subscriptionData.getExpressionType());
            return response;
        }

        MessageFilter messageFilter;
        if (this.brokerController.getBrokerConfig().isFilterSupportRetry()) {
            messageFilter = new ExpressionForRetryMessageFilter(subscriptionData, consumerFilterData, this.brokerController.getConsumerFilterManager());
        } else {
            messageFilter = new ExpressionMessageFilter(subscriptionData, consumerFilterData, this.brokerController.getConsumerFilterManager());
        }

        //核心消息拉取方法
        final GetMessageResult getMessageResult = this.brokerController.getMessageStore().getMessage(
                requestHeader.getConsumerGroup(), requestHeader.getTopic(), requestHeader.getQueueId(),
                requestHeader.getQueueOffset(), requestHeader.getMaxMsgNums(), messageFilter);

        if (getMessageResult != null) {
            response.setRemark(getMessageResult.getStatus().name());
            responseHeader.setNextBeginOffset(getMessageResult.getNextBeginOffset());
            responseHeader.setMinOffset(getMessageResult.getMinOffset());
            responseHeader.setMaxOffset(getMessageResult.getMaxOffset());

            if (getMessageResult.isSuggestPullingFromSlave()) {
                responseHeader.setSuggestWhichBrokerId(subscriptionGroupConfig.getWhichBrokerWhenConsumeSlowly());
            } else {
                responseHeader.setSuggestWhichBrokerId(MixAll.MASTER_ID);
            }

            switch (this.brokerController.getMessageStoreConfig().getBrokerRole()) {
                case SYNC_MASTER:
                case ASYNC_MASTER:
                    break;
                case SLAVE:
                    if (!brokerController.getBrokerConfig().isSlaveReadEnable()) {
                        response.setCode(ResponseCode.PULL_RETRY_IMMEDIATELY);
                        responseHeader.setSuggestWhichBrokerId(MixAll.MASTER_ID);
                    }
                    break;
            }

            if (this.brokerController.getBrokerConfig().isSlaveReadEnable()) {
                if (getMessageResult.isSuggestPullingFromSlave()) {
                    //说明从节点拉取消息慢，从其他节点开始拉取
                    responseHeader.setSuggestWhichBrokerId(subscriptionGroupConfig.getWhichBrokerWhenConsumeSlowly());
                } else {
                    responseHeader.setSuggestWhichBrokerId(subscriptionGroupConfig.getBrokerId());
                }
            } else {
                responseHeader.setSuggestWhichBrokerId(MixAll.MASTER_ID);
            }

            switch (getMessageResult.getStatus()) {
                case FOUND:
                    response.setCode(ResponseCode.SUCCESS);
                    break;
                case MSG_WAS_REMOVING:
                case NO_MATCHED_MSG:
                    response.setCode(ResponseCode.PULL_RETRY_IMMEDIATELY);
                    break;
                case NO_MATCHED_LOGIC_QUEUE:
                case NO_MESSAGE_IN_QUEUE:
                    if (requestHeader.getQueueOffset() != 0) {
                        response.setCode(ResponseCode.PULL_OFFSET_MOVED);
                        log.warn("");
                    } else {
                        response.setCode(ResponseCode.PULL_NOT_FOUND);
                    }
                    break;
                case OFFSET_FOUND_NULL:
                case OFFSET_OVERFLOW_ONE:
                    response.setCode(ResponseCode.PULL_NOT_FOUND);
                    break;
                case OFFSET_OVERFLOW_BADLY:
                    response.setCode(ResponseCode.PULL_OFFSET_MOVED);
                    break;
                case OFFSET_TOO_SMALL:
                    response.setCode(ResponseCode.PULL_OFFSET_MOVED);
                    log.warn("");
                    break;
                default:
                    assert false;
                    break;
            }

            //execute message hook

            switch (response.getCode()) {
                case ResponseCode.SUCCESS:
                    //统计


                    //通过堆内存
                    if (this.brokerController.getBrokerConfig().isTransferMsgByHeap()) {
                        final long beginTimeMillis = System.currentTimeMillis();
                        final byte[] r = this.readGetMessageResult(getMessageResult, requestHeader.getConsumerGroup(), requestHeader.getTopic(), requestHeader.getQueueId());

                        //统计
                        response.setBody(r);
                    } else {
                        try {
                            FileRegion fileRegion = new ManyMessageTransfer(response.encodeHeader(getMessageResult.getBufferTotalSize()), getMessageResult);
                            channel.writeAndFlush(fileRegion).addListener(new ChannelFutureListener() {
                                @Override
                                public void operationComplete(ChannelFuture future) throws Exception {
                                    getMessageResult.release();
                                    if (!future.isSuccess()) {
                                        log.error("transfer many message by pagecache failed: {}", channel.remoteAddress(),  future.cause());
                                    }
                                }
                            });
                        } catch (Exception e) {
                            log.error("", e);
                            getMessageResult.release();
                        }
                    }
                    break;
                case ResponseCode.PULL_NOT_FOUND:
                    //长轮询
                    if (brokerAllowSuspend && hasSuspendFlag) {
                        long pollingTimeMillis = suspendTimeoutMillis;
                        if (!this.brokerController.getBrokerConfig().isLongPollingEnable()) {
                            pollingTimeMillis = this.brokerController.getBrokerConfig().getShortPollingTimeMillis();
                        }
                        String topic = requestHeader.getTopic();
                        long queueOffset = requestHeader.getQueueOffset();
                        int queueId = requestHeader.getQueueId();

                        PullRequest pullRequest = new PullRequest(request, channel, pollingTimeMillis, this.brokerController.getMessageStore().now(),
                                queueOffset, subscriptionData, messageFilter);

                        this.brokerController.getPullHoldService().suspendPullRequest(pullRequest, topic, queueId);
                        response = null;
                        break;
                    }
                case ResponseCode.PULL_RETRY_IMMEDIATELY:
                    break;
                case ResponseCode.PULL_OFFSET_MOVED:
                    if (this.brokerController.getMessageStoreConfig().getBrokerRole() != BrokerRole.SLAVE
                            || this.brokerController.getMessageStoreConfig().isOffsetCheckInSlave()) {
                        MessageQueue mq = new MessageQueue();
                        mq.setTopic(requestHeader.getTopic());
                        mq.setQueueId(requestHeader.getQueueId());
                        mq.setBrokerName(this.brokerController.getBrokerConfig().getBrokerName());

                        OffsetMovedEvent offsetMoveEvent = new OffsetMovedEvent();
                        offsetMoveEvent.setConsumerGroup(requestHeader.getConsumerGroup());
                        offsetMoveEvent.setMessageQueue(mq);
                        offsetMoveEvent.setOffsetRequest(requestHeader.getQueueOffset());
                        offsetMoveEvent.setOffsetNew(getMessageResult.getNextBeginOffset());

                        //mq内部消息
                        this.generateOffsetMovedEvent(offsetMoveEvent);
                        log.warn("PULL_OFFSET_MOVED:correction");
                    } else {
                        responseHeader.setSuggestWhichBrokerId(subscriptionGroupConfig.getBrokerId());
                        response.setCode(ResponseCode.PULL_RETRY_IMMEDIATELY);
                        log.warn("PULL_OFFSET_MOVED:none");
                    }
                    break;
                default:
                    assert false;
                    break;
            }

        } else {
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark("get message from store return null");
        }

        boolean storeOffsetEnable = brokerAllowSuspend;
        storeOffsetEnable = storeOffsetEnable && hasCommitlogFlag;
        storeOffsetEnable = storeOffsetEnable && this.brokerController.getMessageStoreConfig().getBrokerRole() != BrokerRole.SLAVE;

        if (storeOffsetEnable) {
            this.brokerController.getConsumerOffsetManager().commitOffset(RemotingHelper.parseChannel2RemoteAddress(channel), requestHeader.getConsumerGroup(),
                    requestHeader.getTopic(), requestHeader.getQueueId(), requestHeader.getCommitOffset());
        }

        return response;
    }

    private void generateOffsetMovedEvent(OffsetMovedEvent offsetMoveEvent) {
        // try {
        //     MessageExtBrokerInner msgInner = new MessageExtBrokerInner();
        //     msgInner.setTopic(TopicValidator.RMQ_SYS_OFFSET_MOVED_EVENT);
        //     msgInner.setTags(event.getConsumerGroup());
        //     msgInner.setDelayTimeLevel(0);
        //     msgInner.setKeys(event.getConsumerGroup());
        //     msgInner.setBody(event.encode());
        //     msgInner.setFlag(0);
        //     msgInner.setPropertiesString(MessageDecoder.messageProperties2String(msgInner.getProperties()));
        //     msgInner.setTagsCode(MessageExtBrokerInner.tagsString2tagsCode(TopicFilterType.SINGLE_TAG, msgInner.getTags()));
        //
        //     msgInner.setQueueId(0);
        //     msgInner.setSysFlag(0);
        //     msgInner.setBornTimestamp(System.currentTimeMillis());
        //     msgInner.setBornHost(RemotingUtil.string2SocketAddress(this.brokerController.getBrokerAddr()));
        //     msgInner.setStoreHost(msgInner.getBornHost());
        //
        //     msgInner.setReconsumeTimes(0);
        //
        //     PutMessageResult putMessageResult = this.brokerController.getMessageStore().putMessage(msgInner);
        // } catch (Exception e) {
        //     log.warn(String.format("generateOffsetMovedEvent Exception, %s", event.toString()), e);
        // }
    }

    private byte[] readGetMessageResult(final GetMessageResult getResult,final String consumerGroup,
                                        final String topic,final Integer queueId) {
        ByteBuffer buffer = ByteBuffer.allocate(getResult.getBufferTotalSize());
        long storeTimestamp = 0;
        try{
            List<ByteBuffer> msgBufferList = getResult.getMsgBufferList();

            for (ByteBuffer bb : msgBufferList) {
                buffer.put(bb);

                int sysFlag = bb.getInt(MessageDecoder.SYS_FLAG_POSITION);
                int bornHostLength = (sysFlag & MessageSysFlag.FLAG_BORN_HOST_V6) == 0 ? 8 : 20;

                int msgStoreTimePos = MessageStruct.MSG_TOTAL_SIZE +
                        MessageStruct.MSG_MAGIC_CODE +
                        MessageStruct.MSG_BODY_CRC +
                        MessageStruct.MSG_QUEUE_ID +
                        MessageStruct.MSG_FLAG +
                        MessageStruct.MSG_QUEUE_OFFSET +
                        MessageStruct.MSG_PHYSICAL_OFFSET +
                        MessageStruct.MSG_SYS_FLAG +
                        MessageStruct.MSG_BORN_TIMESTAMP +
                        bornHostLength;
                storeTimestamp = bb.getLong(msgStoreTimePos);
            }
        }finally {
            getResult.release();
        }


        //统计

        return buffer.array();
    }

    public void executeRequestWhenWakeUp(final Channel channel, RemotingCommand request) {

    }
}
