package edu.hubu.broker.processor;

import edu.hubu.broker.starter.BrokerController;
import edu.hubu.common.message.MessageQueue;
import edu.hubu.common.protocol.body.LockBatchRequestBody;
import edu.hubu.common.protocol.body.LockBatchResponseBody;
import edu.hubu.common.protocol.header.request.GetMaxOffsetRequestHeader;
import edu.hubu.common.protocol.header.response.GetMaxOffsetResponseHeader;
import edu.hubu.common.protocol.request.RequestCode;
import edu.hubu.remoting.netty.RemotingCommand;
import edu.hubu.remoting.netty.ResponseCode;
import edu.hubu.remoting.netty.exception.RemotingCommandException;
import edu.hubu.remoting.netty.handler.AsyncNettyRequestProcessor;
import edu.hubu.remoting.netty.handler.NettyRequestProcessor;
import io.netty.channel.ChannelHandlerContext;

import java.util.Set;

/**
 * @author: sugar
 * @date: 2023/11/4
 * @description:
 */
public class AdminBrokerProcessor extends AsyncNettyRequestProcessor implements NettyRequestProcessor {

    private final BrokerController brokerController;

    public AdminBrokerProcessor(BrokerController brokerController) {
        this.brokerController = brokerController;
    }

    @Override
    public RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand request) throws Exception {
        switch (request.getCode()){
            case RequestCode.GET_MAX_OFFSET:
                return this.getMaxOffset(ctx, request);
            case RequestCode.LOCK_BATCH_MQ:
                return this.lockBatchMQ(ctx, request);

        }

        return null;
    }


    private RemotingCommand getMaxOffset(ChannelHandlerContext ctx, RemotingCommand request) throws RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(GetMaxOffsetResponseHeader.class);
        final GetMaxOffsetResponseHeader responseHeader = (GetMaxOffsetResponseHeader) response.readCustomHeader();

        final GetMaxOffsetRequestHeader requestHeader = (GetMaxOffsetRequestHeader) request.decodeCustomCommandHeader(GetMaxOffsetRequestHeader.class);

        long offset = this.brokerController.getMessageStore().getMaxOffsetInQueue(requestHeader.getTopic(), requestHeader.getQueueId());
        responseHeader.setOffset(offset);

        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        return response;
    }

    private RemotingCommand lockBatchMQ(final ChannelHandlerContext ctx, final RemotingCommand request){
        RemotingCommand response = RemotingCommand.createResponseCommand(null);
        LockBatchRequestBody requestBody = LockBatchRequestBody.decode(request.getBody(), LockBatchRequestBody.class);

        Set<MessageQueue> lockedSet = this.brokerController.getRebalanceLockManager().tryLockBatch(requestBody.getConsumerGroup(),
                requestBody.getClientId(), requestBody.getMqSet());

        LockBatchResponseBody responseBody = new LockBatchResponseBody();
        responseBody.setLockedMessageQueue(lockedSet);

        response.setBody(responseBody.encode());
        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);

        return response;
    }

    @Override
    public boolean rejectRequest() {
        return false;
    }
}
