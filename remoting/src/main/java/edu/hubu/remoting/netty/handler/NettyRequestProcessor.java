package edu.hubu.remoting.netty.handler;

import edu.hubu.remoting.netty.RemotingCommand;
import io.netty.channel.ChannelHandlerContext;

/**
 * @author: sugar
 * @date: 2023/5/17
 * @description:
 */
public interface NettyRequestProcessor {

    RemotingCommand processRequest(final ChannelHandlerContext ctx, final RemotingCommand request) throws Exception;

    /**
     * when handler rpc command
     * @return
     */
    boolean rejectRequest();

}
