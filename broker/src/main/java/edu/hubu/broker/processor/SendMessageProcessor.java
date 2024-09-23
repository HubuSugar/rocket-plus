package edu.hubu.broker.processor;

import edu.hubu.broker.starter.BrokerController;
import edu.hubu.broker.trace.SendMessageContext;
import edu.hubu.common.TopicConfig;
import edu.hubu.common.message.*;
import edu.hubu.remoting.netty.CustomCommandHeader;
import edu.hubu.common.protocol.SendMessageRequestHeader;
import edu.hubu.common.protocol.header.response.SendMessageResponseHeader;
import edu.hubu.common.protocol.request.RequestCode;
import edu.hubu.remoting.netty.RemotingCommand;
import edu.hubu.remoting.netty.ResponseCode;
import edu.hubu.remoting.netty.exception.RemotingCommandException;
import edu.hubu.remoting.netty.handler.NettyRequestProcessor;
import edu.hubu.remoting.netty.handler.ResponseInvokeCallback;
import edu.hubu.store.PutMessageResult;
import io.netty.channel.ChannelHandlerContext;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * @author: sugar
 * @date: 2023/5/24
 * @description:
 */
@Slf4j
public class SendMessageProcessor extends AbstractSendMessageProcessor implements NettyRequestProcessor {

    public SendMessageProcessor(BrokerController brokerController) {
        super(brokerController);
    }

    @Override
    public RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand request) throws Exception {
        log.info("处理发送消息请求....{}", request);
        RemotingCommand response = null;
        try{
            response = asyncProcessRequest(ctx, request).get();
        }catch (Exception e){
            log.error("process sendMessage error", e);
        }
        return response;
    }

    @Override
    public boolean rejectRequest() {
        return false;
    }

    @Override
    public void processRequestAsync(ChannelHandlerContext ctx, RemotingCommand request, ResponseInvokeCallback callback) throws Exception {
        super.processRequestAsync(ctx, request, callback);
    }



    private CompletableFuture<RemotingCommand> asyncProcessRequest(ChannelHandlerContext ctx, RemotingCommand request) throws RemotingCommandException {
        SendMessageRequestHeader requestHeader = parseRequestHeader(request);
        final SendMessageContext traceContext = buildMsgTraceContext(ctx, requestHeader);
        switch (request.getCode()){
            case RequestCode.SEND_MESSAGE_BATCH:
                return this.asyncSendMessageBatch(ctx, request, requestHeader, traceContext);
            default:
                return this.asyncSendMessage(ctx, request, requestHeader, traceContext);
        }

    }


    private SendMessageContext buildMsgTraceContext(ChannelHandlerContext ctx, CustomCommandHeader requestHeader){
        return null;
    }

    private CompletableFuture<RemotingCommand> asyncSendMessage(ChannelHandlerContext ctx, RemotingCommand request, SendMessageRequestHeader requestHeader, SendMessageContext context){
        //1、创建响应
        RemotingCommand response = preSend(ctx, request, requestHeader);
        SendMessageResponseHeader responseHeader = (SendMessageResponseHeader) response.readCustomHeader();
        if(response.getCode() != -1){
            return CompletableFuture.completedFuture(response);
        }
        byte[] msgBody = request.getBody();

        TopicConfig config = this.brokerController.getTopicConfigManager().selectTopicConfig(requestHeader.getTopic());
        int queueId = requestHeader.getQueueId();
        if(queueId < 0){
            queueId = randomQueueId(config.getWriteQueueNums());
        }

        MessageExtBrokerInner msgInner = new MessageExtBrokerInner();
        msgInner.setTopic(requestHeader.getTopic());
        msgInner.setQueueId(queueId);

        if(!handleRetryAndDLQ(requestHeader, response, request, msgInner, config)){
            return CompletableFuture.completedFuture(response);
        }

        msgInner.setBody(msgBody);
        //设置顶级父类Message中的flag
        msgInner.setFlag(requestHeader.getFlag());
        //设置消息的属性
        MessageAccessor.setProperties(msgInner, MessageDecoder.string2Properties(requestHeader.getProperties()));
        msgInner.setPropertiesString(requestHeader.getProperties());

        msgInner.setBornTimestamp(requestHeader.getBornTimestamp());
        msgInner.setBornHost(ctx.channel().remoteAddress());
        msgInner.setStoreHost(this.storeHost);
        msgInner.setReconsumeTimes(requestHeader.getReconsumeTimes() == null ? 0 : requestHeader.getReconsumeTimes());

        String clusterName = this.brokerController.getBrokerConfig().getBrokerClusterName();
        MessageAccessor.putProperty(msgInner, MessageConst.PROPERTY_CLUSTER, clusterName);
        msgInner.setPropertiesString(MessageDecoder.properties2String(msgInner.getProperties()));

        CompletableFuture<PutMessageResult> putMessageResult = null;
        Map<String,String> properties = MessageDecoder.string2Properties(requestHeader.getProperties());
        String transFlag = properties.get(MessageConst.PROPERTY_TRANSACTION_PREPARE);
        if(Boolean.parseBoolean(transFlag)){
            //事务消息

        }else{
            putMessageResult = this.brokerController.getMessageStore().asyncPutMessage(msgInner);
        }

        return handlePutMessageResultFuture(putMessageResult, response, request, responseHeader, ctx, context, msgInner, queueId);
    }


    private CompletableFuture<RemotingCommand> asyncSendMessageBatch(ChannelHandlerContext ctx, RemotingCommand request, SendMessageRequestHeader requestHeader, SendMessageContext context){

        return null;
    }

    private CompletableFuture<RemotingCommand> handlePutMessageResultFuture(CompletableFuture<PutMessageResult> future,
                                                                            RemotingCommand response, RemotingCommand request,
                                                                            SendMessageResponseHeader responseHeader, ChannelHandlerContext ctx,
                                                                            SendMessageContext context, MessageExt messageExt, int queueId){
        return future.thenApply(result -> handlePutMessageResult(result, request, response,responseHeader, ctx, queueId, context, messageExt));
    }
    private RemotingCommand handlePutMessageResult(PutMessageResult result, RemotingCommand request, RemotingCommand response, SendMessageResponseHeader responseHeader,
                                                   ChannelHandlerContext ctx, int queueId, SendMessageContext context, MessageExt message){
        if(result == null){
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark("store put message return null");
            return response;
        }
        boolean sendOk = convertPutMessageResult(result, response);

        if(sendOk){
            response.setRemark(null);

            responseHeader.setMsgId(result.getAppendMessageResult().getMsgId());
            responseHeader.setQueueId(queueId);
            responseHeader.setQueueOffset(result.getAppendMessageResult().getLogicOffset());

            doResponse(ctx, request, response);
            return null;
        }else{

        }

        return response;
    }

    private int randomQueueId(int writeQueueNums){
        return (this.random.nextInt() % 99999999) % writeQueueNums;
    }

    /**
     * 构造响应response
     * 进行消息检查
     * @param ctx
     * @param request
     * @param requestHeader
     * @return
     */
    private RemotingCommand preSend(ChannelHandlerContext ctx, RemotingCommand request, SendMessageRequestHeader requestHeader){
        RemotingCommand response = RemotingCommand.createResponseCommand(SendMessageResponseHeader.class);
        response.setOpaque(request.getOpaque());
        //增加消息的区域和traceOn属性
        // response.addExtField();
        // response.addExtField();

        response.setCode(-1);

        super.msgCheck(ctx, requestHeader, response);
        return response;
    }

    /**
     * 处理重试消息和死信队列
     * @param requestHeader
     * @param response
     * @param request
     * @param message
     * @return
     */
    private boolean handleRetryAndDLQ(SendMessageRequestHeader requestHeader, RemotingCommand response, RemotingCommand request, MessageExt message, TopicConfig config){

        return true;
    }

    private String diskUtil(){
        return "";
    }

    private boolean convertPutMessageResult(PutMessageResult result, RemotingCommand response){
        boolean sendOk = false;
        switch (result.getPutMessageStatus()){
            //success状态
            case PUT_OK:
                response.setCode(ResponseCode.SUCCESS);
                sendOk = true;
                break;
            case FLUSH_DISK_TIMEOUT:
                response.setCode(ResponseCode.FLUSH_DISK_TIMEOUT);
                sendOk = true;
                break;
            case FLUSH_SLAVE_TIMEOUT:
                response.setCode(ResponseCode.FLUSH_SLAVE_TIMEOUT);
                sendOk = true;
                break;
            case SLAVE_NOT_AVAILABLE:
                response.setCode(ResponseCode.SLAVE_NOT_AVAILABLE);
                sendOk = true;
                break;
            //fail状态
            case CREATE_MAPPED_FILE_FAILED:
                response.setCode(ResponseCode.SYSTEM_ERROR);
                response.setRemark("create mappedFile failed, server is busy or broken.");
                break;
            case MESSAGE_ILLEGAL:
            case PROPERTIES_SIZE_EXCEED:
                response.setCode(ResponseCode.MESSAGE_ILLEGAL);
                response.setRemark("the message is illegal, maybe msg body or properties length not matched, msg body length limit 128k, msg properties length limit 32k.");
                break;
            case SERVICE_NOT_AVAILABLE:
                response.setCode(ResponseCode.SERVICE_NOT_AVAILABLE);
                response.setRemark("service not available now, maybe disk full." + diskUtil() + ", maybe your broker machine memory too small.");
                break;
            case OS_PAGECACHE_BUSY:
                response.setCode(ResponseCode.SYSTEM_ERROR);
                response.setRemark("[PC_SYNCHRONIZED]broker busy, start flow control fow a while");
                break;
            case UNKNOWN_ERROR:
                response.setCode(ResponseCode.SYSTEM_ERROR);
                response.setRemark("unknown error");
                break;
            default:
                response.setCode(ResponseCode.SYSTEM_ERROR);
                response.setRemark("unknown error default");
                break;
        }
        return sendOk;
    }
}
