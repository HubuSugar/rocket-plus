package edu.hubu.remoting.netty.handler;

import edu.hubu.remoting.netty.RemotingCommand;
import io.netty.channel.ChannelHandlerContext;

/**
 * @author: sugar
 * @date: 2023/5/19
 * @description:
 */
public abstract class AsyncNettyRequestProcessor implements NettyRequestProcessor{

    public void processRequestAsync(final ChannelHandlerContext ctx, final RemotingCommand request, final ResponseInvokeCallback callback) throws Exception {
        RemotingCommand response = processRequest(ctx, request);
        callback.callback(response);
    }

}
