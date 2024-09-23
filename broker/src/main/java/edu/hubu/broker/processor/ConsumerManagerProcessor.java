package edu.hubu.broker.processor;

import edu.hubu.broker.client.ConsumerGroupInfo;
import edu.hubu.broker.starter.BrokerController;
import edu.hubu.common.protocol.header.request.GetConsumerIdListByGroupRequestHeader;
import edu.hubu.common.protocol.body.GetConsumerIdListByGroupResponseBody;
import edu.hubu.common.protocol.header.response.GetConsumerIdListByGroupResponseHeader;
import edu.hubu.common.protocol.request.RequestCode;
import edu.hubu.remoting.netty.RemotingCommand;
import edu.hubu.remoting.netty.common.RemotingHelper;
import edu.hubu.remoting.netty.ResponseCode;
import edu.hubu.remoting.netty.exception.RemotingCommandException;
import edu.hubu.remoting.netty.handler.AsyncNettyRequestProcessor;
import edu.hubu.remoting.netty.handler.NettyRequestProcessor;
import io.netty.channel.ChannelHandlerContext;
import lombok.extern.slf4j.Slf4j;

import java.util.List;

/**
 * @author: sugar
 * @date: 2023/11/4
 * @description: 消费者管理processor
 */
@Slf4j
public class ConsumerManagerProcessor extends AsyncNettyRequestProcessor implements NettyRequestProcessor {

    private final BrokerController brokerController;

    public ConsumerManagerProcessor(BrokerController brokerController) {
        this.brokerController = brokerController;
    }

    @Override
    public RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand request) throws Exception {
        switch (request.getCode()){
            case RequestCode
                    .GET_CONSUMER_LIST_BY_GROUP:
                return this.getConsumerListByGroup(ctx, request);
        }
        return null;
    }


    /**
     * 根据消费者或者客户端id
     * @param ctx
     * @param request
     * @return
     */
    private RemotingCommand getConsumerListByGroup(ChannelHandlerContext ctx, RemotingCommand request) throws RemotingCommandException {
        final GetConsumerIdListByGroupRequestHeader requestHeader = (GetConsumerIdListByGroupRequestHeader) request.decodeCustomCommandHeader(GetConsumerIdListByGroupRequestHeader.class);
        final RemotingCommand response = RemotingCommand.createResponseCommand(GetConsumerIdListByGroupResponseHeader.class);

        final ConsumerGroupInfo consumerGroupInfo = this.brokerController
                .getConsumerManager().getConsumerGroupInfo(requestHeader.getConsumerGroup());

        if(consumerGroupInfo != null){
            List<String> clientIds = consumerGroupInfo.getAllClientId();
            if(!clientIds.isEmpty()){
                GetConsumerIdListByGroupResponseBody responseBody = new GetConsumerIdListByGroupResponseBody();
                responseBody.setConsumerIdList(clientIds);
                response.setBody(responseBody.encode());
                response.setCode(ResponseCode.SUCCESS);
                response.setRemark(null);
                return response;
            }else{
                log.warn("get all client ids failed, {}, {}", requestHeader.getConsumerGroup(), RemotingHelper.parseChannel2RemoteAddress(ctx.channel()));
            }
        }else{
            log.warn("get consumer group info failed, consumerGroup: {}, channel: {}", requestHeader.getConsumerGroup(), RemotingHelper.parseChannel2RemoteAddress(ctx.channel()));
        }

        response.setCode(ResponseCode.SYSTEM_ERROR);
        response.setRemark("no consumer for this group, " + requestHeader.getConsumerGroup());

        return response;
    }

    @Override
    public boolean rejectRequest() {
        return false;
    }
}
