package edu.hubu.namesrv.processor;

import edu.hubu.common.DataVersion;
import edu.hubu.common.protocol.body.RegisterBrokerBody;
import edu.hubu.common.protocol.header.request.GetTopicRouteInfoHeader;
import edu.hubu.common.protocol.header.request.QueryDataVersionRequestHeader;
import edu.hubu.common.protocol.header.request.RegisterBrokerRequestHeader;
import edu.hubu.common.protocol.header.response.QueryDataVersionResponseHeader;
import edu.hubu.common.protocol.header.response.RegisterBrokerResponseHeader;
import edu.hubu.common.protocol.request.RequestCode;
import edu.hubu.common.protocol.result.RegisterBrokerResult;
import edu.hubu.common.protocol.route.TopicRouteData;
import edu.hubu.namesrv.NamesrvController;
import edu.hubu.namesrv.util.NamesvUtil;
import edu.hubu.remoting.netty.RemotingCommand;
import edu.hubu.remoting.netty.ResponseCode;
import edu.hubu.remoting.netty.handler.NettyRequestProcessor;
import io.netty.channel.ChannelHandlerContext;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.atomic.AtomicLong;

/**
 * @author: sugar
 * @date: 2023/6/6
 * @description:
 */
@Slf4j
public class DefaultRequestProcessor implements NettyRequestProcessor {

    private final NamesrvController namesrvController;

    public DefaultRequestProcessor(NamesrvController namesrvController) {
        this.namesrvController = namesrvController;
    }

    @Override
    public RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand request) throws Exception {
        switch (request.getCode()){
            case RequestCode.QUERY_DATA_VERSION:
                return queryBrokerTopicConfig(ctx, request);
            case RequestCode.GET_TOPIC_ROUTE:
                return getTopicRouteInfo(ctx, request);
            case RequestCode.REGISTER_BROKER:
                return registerAllBroker(ctx, request);
            default:
                break;
        }
        return null;
    }

    public RemotingCommand getTopicRouteInfo(final ChannelHandlerContext ctx, final RemotingCommand request) throws Exception{
        RemotingCommand response = RemotingCommand.createResponseCommand(null);
        final GetTopicRouteInfoHeader topicRouteInfoHeader = (GetTopicRouteInfoHeader) request.decodeCustomCommandHeader(GetTopicRouteInfoHeader.class);

        TopicRouteData topicRouteData = this.namesrvController.getTopicRouteInfoManager().pickupRouteData(topicRouteInfoHeader.getTopic());
        if(topicRouteData != null){
            if(namesrvController.getNamesrvConfig().isOrderMessageEnable()){
                String orderTopicConf = namesrvController.getKvConfigManager().getKVConfig(NamesvUtil.ORDER_TOPIC_CONFIG, topicRouteInfoHeader.getTopic());
                topicRouteData.setOrderTopicConf(orderTopicConf);
            }

            byte[] content = topicRouteData.encode();
            response.setBody(content);
            response.setCode(ResponseCode.SUCCESS);
            response.setRemark(null);
            return response;
        }

        response.setCode(ResponseCode.TOPIC_NOT_EXIST);
        response.setRemark("no topic route info");

        return response;
    }

    public RemotingCommand registerAllBroker(final ChannelHandlerContext ctx, final RemotingCommand request)  {

        RemotingCommand response = RemotingCommand.createResponseCommand(RegisterBrokerResponseHeader.class);
        RegisterBrokerResponseHeader responseHeader = (RegisterBrokerResponseHeader)response.readCustomHeader();
        RegisterBrokerRequestHeader requestHeader = (RegisterBrokerRequestHeader) request.decodeCustomCommandHeader(RegisterBrokerRequestHeader.class);

        RegisterBrokerBody registerBrokerBody = new RegisterBrokerBody();
        if(request.getBody() != null){
            log.info("request body, {}", new String(request.getBody()));
            registerBrokerBody = RegisterBrokerBody.decode(request.getBody(), requestHeader.isCompressed());
        }else{
            registerBrokerBody.getTopicConfigSerializeWrapper().getDataVersion().setTimestamp(0);
            registerBrokerBody.getTopicConfigSerializeWrapper().getDataVersion().setCounter(new AtomicLong(0));
        }

        RegisterBrokerResult registerResult = this.namesrvController.getTopicRouteInfoManager().registerAllBroker(
                requestHeader.getBrokerName(),
                requestHeader.getBrokerAddress(),
                requestHeader.getClusterName(),
                requestHeader.getBrokerId(),
                requestHeader.getHaServerAddr(),
                registerBrokerBody.getTopicConfigSerializeWrapper(),
                registerBrokerBody.getFilterServerList(),
                ctx.channel()
        );
        responseHeader.setHaServer(registerResult.getHaServer());
        responseHeader.setMasterAddress(registerResult.getMasterAddr());

        //获取顺序topic的配置

        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        return response;
    }

    public RemotingCommand queryBrokerTopicConfig(final ChannelHandlerContext ctx, final RemotingCommand request){
        RemotingCommand response = RemotingCommand.createResponseCommand(QueryDataVersionResponseHeader.class);
        QueryDataVersionResponseHeader responseHeader = (QueryDataVersionResponseHeader) response.readCustomHeader();
        QueryDataVersionRequestHeader requestHeader = (QueryDataVersionRequestHeader) request.decodeCustomCommandHeader(QueryDataVersionRequestHeader.class);
        DataVersion dataVersion = DataVersion.decode(request.getBody(), DataVersion.class);

        Boolean changed = this.namesrvController.getTopicRouteInfoManager().isBrokerTopicConfigChanged(requestHeader.getBrokerAddress(), dataVersion);
        if(!changed){
            this.namesrvController.getTopicRouteInfoManager().updateBrokerInfoUpdateTimestamp(requestHeader.getBrokerAddress());
        }

        DataVersion nameSrvDataVersion = this.namesrvController.getTopicRouteInfoManager().queryTopicConfig(requestHeader.getBrokerAddress());
        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);

        if(nameSrvDataVersion != null){
            response.setBody(nameSrvDataVersion.encode());
        }
        responseHeader.setChanged(changed);
        return  response;
    }

    @Override
    public boolean rejectRequest() {
        return false;
    }
}
