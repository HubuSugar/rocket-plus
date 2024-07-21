package edu.hubu.broker.processor;

import edu.hubu.broker.starter.BrokerController;
import edu.hubu.remoting.netty.RemotingCommand;
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
    public RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand request) throws Exception {
        return null;
    }

    @Override
    public boolean rejectRequest() {
        return false;
    }
}
