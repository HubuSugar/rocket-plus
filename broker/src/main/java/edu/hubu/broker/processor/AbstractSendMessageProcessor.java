package edu.hubu.broker.processor;

import edu.hubu.broker.starter.BrokerController;
import edu.hubu.common.PermName;
import edu.hubu.common.TopicConfig;
import edu.hubu.common.protocol.SendMessageRequestHeader;
import edu.hubu.remoting.netty.RemotingCommand;
import edu.hubu.remoting.netty.RemotingHelper;
import edu.hubu.remoting.netty.ResponseCode;
import edu.hubu.remoting.netty.handler.AsyncNettyRequestProcessor;
import edu.hubu.remoting.netty.handler.NettyRequestProcessor;
import io.netty.channel.ChannelHandlerContext;
import lombok.extern.slf4j.Slf4j;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Random;

/**
 * @author: sugar
 * @date: 2023/7/11
 * @description:
 */
@Slf4j
public abstract class AbstractSendMessageProcessor extends AsyncNettyRequestProcessor implements NettyRequestProcessor {

    protected final BrokerController brokerController;
    protected final Random random = new Random(System.currentTimeMillis());
    protected final SocketAddress storeHost;

    public AbstractSendMessageProcessor(final BrokerController brokerController) {
        this.brokerController = brokerController;
        this.storeHost = new InetSocketAddress(this.brokerController.getBrokerConfig().getBrokerIP1(), this.brokerController.getNettyServerConfig().getListenPort());
    }

    /**
     * todo
     * 根据requestCode的batchMessage、sendMessage、sendMessageV2来区分
     * @param request
     * @return
     */
    protected SendMessageRequestHeader parseRequestHeader(RemotingCommand request){
        return (SendMessageRequestHeader) request.decodeCustomCommandHeader(SendMessageRequestHeader.class);
    }


    protected RemotingCommand msgCheck(ChannelHandlerContext ctx, SendMessageRequestHeader requestHeader, RemotingCommand response){
        //1、判断broker的写权限
        if(!PermName.isWritable(this.brokerController.getBrokerConfig().getBrokerPermission())
            && this.brokerController.getTopicConfigManager().isOrderTopic(requestHeader.getTopic())){
            response.setCode(ResponseCode.NO_PERMISSION);
            response.setRemark("no permission to send to this broker: " + this.brokerController.getBrokerConfig().getBrokerIP1());
            return response;
        }
        //2、校验topic的合法性
        //3、校验当前topic的配置topicConfig
        TopicConfig config = this.brokerController.getTopicConfigManager().selectTopicConfig(requestHeader.getTopic());
        if(config == null){
            int topicSysFlag = 0;
            //

           config =  this.brokerController.getTopicConfigManager().createTopicInSendMessageMethod(
                    requestHeader.getTopic(),
                    requestHeader.getDefaultTopic(),
                    requestHeader.getDefaultTopicQueueNums(),
                    RemotingHelper.parseChannel2RemoteAddress(ctx.channel()),
                    topicSysFlag
            );

           if(config == null){
               response.setCode(ResponseCode.TOPIC_NOT_EXIST);
               response.setRemark("topic " + requestHeader.getTopic() + " does not exist");
               return response;
           }
        }

        int queueId = requestHeader.getQueueId();
        int maxQueueId = Math.max(config.getWriteQueueNums(), config.getReadQueueNums());
        if(queueId >= maxQueueId){
            String error = String.format("queueId %d is illegal, topic config :%s, producer: %s", queueId, config.toString(),RemotingHelper.parseChannel2RemoteAddress(ctx.channel()));
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark(error);
            return response;
        }

        return response;
    }

    protected void doResponse(ChannelHandlerContext ctx, RemotingCommand request, final RemotingCommand response){
       if(!request.isOnewayType()){
           try{
               ctx.writeAndFlush(response);
           }catch (Throwable e){
               log.error("sendMessageProcessor process request over, but response failed", e);
               log.error(request.toString());
               log.error(response.toString());
           }
       }
    }
}
