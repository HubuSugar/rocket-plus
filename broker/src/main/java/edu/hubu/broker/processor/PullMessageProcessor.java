package edu.hubu.broker.processor;

import edu.hubu.broker.starter.BrokerController;
import edu.hubu.remoting.netty.RemotingCommand;
import edu.hubu.remoting.netty.handler.AsyncNettyRequestProcessor;
import edu.hubu.remoting.netty.handler.NettyRequestProcessor;
import edu.hubu.remoting.netty.handler.ResponseInvokeCallback;
import io.netty.channel.ChannelHandlerContext;

/**
 * @author: sugar
 * @date: 2023/11/1
 * @description:
 */
public class PullMessageProcessor extends AsyncNettyRequestProcessor implements NettyRequestProcessor {

    private final BrokerController brokerController;

    public PullMessageProcessor(BrokerController brokerController) {
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
