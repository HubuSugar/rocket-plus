package edu.hubu.broker.processor;

import edu.hubu.broker.client.ClientChannelInfo;
import edu.hubu.broker.starter.BrokerController;
import edu.hubu.common.PermName;
import edu.hubu.common.protocol.heartbeat.ConsumerData;
import edu.hubu.common.protocol.heartbeat.HeartbeatData;
import edu.hubu.common.protocol.heartbeat.ProducerData;
import edu.hubu.common.protocol.request.RequestCode;
import edu.hubu.common.subcription.SubscriptionGroupConfig;
import edu.hubu.common.topic.TopicSysFlag;
import edu.hubu.common.utils.MixAll;
import edu.hubu.remoting.netty.RemotingCommand;
import edu.hubu.remoting.netty.RemotingHelper;
import edu.hubu.remoting.netty.ResponseCode;
import edu.hubu.remoting.netty.exception.RemotingCommandException;
import edu.hubu.remoting.netty.handler.AsyncNettyRequestProcessor;
import edu.hubu.remoting.netty.handler.NettyRequestProcessor;
import io.netty.channel.ChannelHandlerContext;
import lombok.extern.slf4j.Slf4j;

/**
 * @author: sugar
 * @date: 2024/7/21
 * @description: 心跳处理processor
 */
@Slf4j
public class ClientManageProcessor extends AsyncNettyRequestProcessor implements NettyRequestProcessor {

    private final BrokerController brokerController;

    public ClientManageProcessor(final BrokerController brokerController) {
        this.brokerController = brokerController;
    }

    @Override
    public RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand request) throws RemotingCommandException {
        switch (request.getCode()) {
            case RequestCode
                    .HEART_BEAT:
                return this.heartbeat(ctx, request);
            default:
                break;
        }
        return null;
    }

    @Override
    public boolean rejectRequest() {
        return false;
    }

    public RemotingCommand heartbeat(final ChannelHandlerContext ctx,final RemotingCommand cmd){
        RemotingCommand responseCommand = RemotingCommand.createResponseCommand(null);
        HeartbeatData heartbeatData = HeartbeatData.decode(responseCommand.getBody(), HeartbeatData.class);
        ClientChannelInfo clientChannelInfo = new ClientChannelInfo(
                ctx.channel(),
                heartbeatData.getClientId(),
                cmd.getLanguage(),
                cmd.getVersion());

        //consumer
        for (ConsumerData consumeDatum : heartbeatData.getConsumeData()) {
            SubscriptionGroupConfig subscriptionGroupConfig = this.brokerController.getSubscriptionGroupManager().findSubscriptionGroupConfig(consumeDatum.getGroupName());
            boolean isNotifyConsumerIdsChangeEnable = true;
            if(subscriptionGroupConfig != null){
                isNotifyConsumerIdsChangeEnable = subscriptionGroupConfig.isNotifyConsumerIdsChangeEnable();
                int topicSysFlag = 0;
                if(consumeDatum.isUnitMode()){
                    topicSysFlag = TopicSysFlag.buildSysFlag(false, true);
                }
                String newTopic = MixAll.getRetryTopic(consumeDatum.getGroupName());
                this.brokerController.getTopicConfigManager().createTopicInSendMessageBackMethod(
                        newTopic,
                        subscriptionGroupConfig.getRetryQueueNums(),
                        PermName.PERM_WRITABLE | PermName.PERM_READABLE,
                        topicSysFlag
                );

                boolean changed = this.brokerController.getConsumerManager().registerConsumer(consumeDatum.getGroupName(),
                        clientChannelInfo, consumeDatum.getConsumerType(), consumeDatum.getMessageModel(),
                        consumeDatum.getConsumeFromWhere(), consumeDatum.getSubscriptionData(), isNotifyConsumerIdsChangeEnable);

                if(changed){
                    log.info("register consumer info changed, date: {}, channel:{}", consumeDatum, RemotingHelper.parseChannel2RemoteAddress(ctx.channel()));
                }

            }
        }

        //producer
        for (ProducerData produceDatum : heartbeatData.getProduceData()) {
            this.brokerController.getProducerManager().registerProducer(produceDatum.getGroupName(), clientChannelInfo);
        }

        responseCommand.setCode(ResponseCode.SUCCESS);
        responseCommand.setRemark(null);
        return responseCommand;
    }
}
