package edu.hubu.client.impl.consumer;

import edu.hubu.client.consumer.PullCallback;
import edu.hubu.client.consumer.PullResult;
import edu.hubu.client.consumer.PullStatus;
import edu.hubu.client.exception.MQClientException;
import edu.hubu.client.hook.FilterMessageHook;
import edu.hubu.client.impl.CommunicationMode;
import edu.hubu.client.impl.FindBrokerResult;
import edu.hubu.client.instance.MQClientInstance;
import edu.hubu.common.exception.broker.MQBrokerException;
import edu.hubu.common.filter.ExpressionType;
import edu.hubu.common.message.*;
import edu.hubu.common.protocol.header.request.PullMessageRequestHeader;
import edu.hubu.common.protocol.heartbeat.SubscriptionData;
import edu.hubu.common.protocol.route.TopicRouteData;
import edu.hubu.common.sysFlag.PullSysFlag;
import edu.hubu.common.utils.MixAll;
import edu.hubu.remoting.netty.exception.RemotingException;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author: sugar
 * @date: 2023/10/28
 * @description:
 */
public class PullAPIWrapper {

    private final MQClientInstance mqClientInstance;
    private final String consumerGroup;
    private final boolean unitMode;

    private final ConcurrentHashMap<MessageQueue, AtomicLong> pullFromWhichNodeTable = new ConcurrentHashMap<>(32);
    private volatile boolean connectBrokerByUser = false;
    private volatile long defaultBrokerId = MixAll.MASTER_ID;
    private final Random random = new Random(System.currentTimeMillis());

    public PullAPIWrapper(MQClientInstance mqClientInstance, String consumerGroup, boolean unitMode) {
        this.mqClientInstance = mqClientInstance;
        this.consumerGroup = consumerGroup;
        this.unitMode = unitMode;
    }


    public PullResult processPullResult(final MessageQueue mq,final PullResult pullResult,final SubscriptionData subscriptionData) {
        PullResultExt pullResultExt = (PullResultExt) pullResult;
        this.updatePullFromWhichNode(mq, pullResultExt.getSuggestWhichBrokerId());

        if(PullStatus.FOUND == pullResult.getPullStatus()){
            ByteBuffer byteBuffer = ByteBuffer.wrap(pullResultExt.getMessageBinary());
            List<MessageExt> msgList = MessageDecoder.decodes(byteBuffer);

            List<MessageExt> messageFilterAgain = msgList;
            if(!subscriptionData.getTagSet().isEmpty() && !subscriptionData.isClassFilterMode()){
                messageFilterAgain = new ArrayList<>(msgList.size());
                for (MessageExt msg : msgList) {
                    if(msg.getTags() != null){
                        if(subscriptionData.getTagSet().contains(msg.getTags())){
                            messageFilterAgain.add(msg);
                        }
                    }
                }
            }

            //todo exec message hook

            for (MessageExt msg : messageFilterAgain) {
                String tranFlag = msg.getProperty(MessageConst.PROPERTY_TRANSACTION_PREPARE);
                if(Boolean.parseBoolean(tranFlag)){
                    msg.setTransactionId(msg.getProperty(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX));
                }
                MessageAccessor.putProperty(msg, MessageConst.PROPERTY_MIN_OFFSET, Long.toString(pullResult.getMinOffset()));
                MessageAccessor.putProperty(msg, MessageConst.PROPERTY_MAX_OFFSET, Long.toString(pullResult.getMaxOffset()));
                msg.setBrokerName(mq.getBrokerName());
            }

            pullResultExt.setMsgFoundList(messageFilterAgain);
        }
        pullResultExt.setMessageBinary(null);

        return pullResult;
    }

    private void updatePullFromWhichNode(final MessageQueue mq,final long brokerId) {
        AtomicLong suggest = this.pullFromWhichNodeTable.get(mq);
        if(null == suggest){
            this.pullFromWhichNodeTable.put(mq, new AtomicLong(brokerId));
        }else{
            suggest.set(brokerId);
        }
    }

    public void registerFilterMessageHook(ArrayList<FilterMessageHook> filterMessageHookList) {

    }

    public MQClientInstance getMqClientInstance() {
        return mqClientInstance;
    }

    public String getConsumerGroup() {
        return consumerGroup;
    }

    public boolean isUnitMode() {
        return unitMode;
    }


    public PullResult pullKernelImpl(final MessageQueue mq,
                                     final String subString,
                                     final String expressionType,
                                     final long subVersion,
                                     final long offset,
                                     final int maxNums,
                                     final int sysFlag,
                                     final long commitOffset,
                                     final long brokerSuspendMaxTimeMillis,
                                     final long timeoutMillis,
                                     final CommunicationMode communicationMode,
                                     final PullCallback pullCallback) throws MQClientException, MQBrokerException, RemotingException, InterruptedException {
        FindBrokerResult findBrokerResult = this.mqClientInstance.findBrokerAddressInSubscribe(mq.getBrokerName(),
                this.recalculatePullFromWhichNode(mq), false);
        if(findBrokerResult == null){
            this.mqClientInstance.updateTopicInfoFromNameServer(mq.getTopic());
            findBrokerResult = this.mqClientInstance.findBrokerAddressInSubscribe(mq.getBrokerName(),
                    this.recalculatePullFromWhichNode(mq), false);
        }

        if(findBrokerResult != null){
            {
                //checkVersion
                // if(!ExpressionType.isTagType(expressionType)){
                //
                //
                // }

            }

            int sysFlagInner = sysFlag;

            if(findBrokerResult.isSlave()){
                sysFlagInner = PullSysFlag.clearCommitlogOffsetFlag(sysFlagInner);
            }

            PullMessageRequestHeader requestHeader = new PullMessageRequestHeader();
            requestHeader.setConsumerGroup(this.consumerGroup);
            requestHeader.setTopic(mq.getTopic());
            requestHeader.setQueueId(mq.getQueueId());
            requestHeader.setQueueOffset(offset);
            requestHeader.setMaxMsgNums(maxNums);
            requestHeader.setSysFlag(sysFlagInner);
            requestHeader.setCommitOffset(commitOffset);
            requestHeader.setSuspendTimeoutMillis(brokerSuspendMaxTimeMillis);
            requestHeader.setSubscription(subString);
            requestHeader.setSubVersion(subVersion);
            requestHeader.setExpressionType(expressionType);

            String brokerAddress = findBrokerResult.getBrokerAddress();

            if(PullSysFlag.hasClassFilterFlag(sysFlagInner)){
                brokerAddress = computePullFromWhichFilterServer(mq.getTopic(), brokerAddress);
            }

            return this.mqClientInstance.getMqClientAPI().pullMessage(brokerAddress,
                    requestHeader,
                    timeoutMillis,
                    communicationMode,
                    pullCallback
                    );
        }

        throw new MQClientException("the broker["+ mq.getBrokerName() +"] not exist", null);
    }

    private String computePullFromWhichFilterServer(String topic, String brokerAddress) throws MQClientException {
        ConcurrentMap<String, TopicRouteData> topicRouteTable = this.mqClientInstance.getTopicRouteTable();
        if(topicRouteTable != null){
            TopicRouteData topicRouteData = topicRouteTable.get(topic);
            List<String> list = topicRouteData.getFilterServerTable().get(brokerAddress);

            if(list != null && !list.isEmpty()){
                return list.get(randomNum() % list.size());
            }

        }
        throw new MQClientException("find filter server failed, broker address: " + brokerAddress, null);
    }

    public long recalculatePullFromWhichNode(MessageQueue mq) {
        if(this.isConnectBrokerByUser()){
            return this.defaultBrokerId;
        }

        AtomicLong suggest =this.pullFromWhichNodeTable.get(mq);
        if(suggest != null){
            return suggest.get();
        }

        return MixAll.MASTER_ID;
    }

    public int randomNum(){
        int value = random.nextInt();
        if(value < 0){
            value = Math.abs(value);
            if(value < 0){
                value = 0;
            }
        }
        return value;
    }

    public ConcurrentHashMap<MessageQueue, AtomicLong> getPullFromWhichNodeTable() {
        return pullFromWhichNodeTable;
    }

    public boolean isConnectBrokerByUser() {
        return connectBrokerByUser;
    }

    public void setConnectBrokerByUser(boolean connectBrokerByUser) {
        this.connectBrokerByUser = connectBrokerByUser;
    }

    public long getDefaultBrokerId() {
        return defaultBrokerId;
    }

    public void setDefaultBrokerId(long defaultBrokerId) {
        this.defaultBrokerId = defaultBrokerId;
    }
}
