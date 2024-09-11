package edu.hubu.remoting.netty.handler;

import edu.hubu.remoting.netty.RemotingCommand;

/**
 * @author: sugar
 * @date: 2023/5/19
 * @description:
 */
public interface RpcHook {

    void doBeforeHook(final String remoteAddr, final RemotingCommand request);

    void doAfterHook(final String remoteAddr, final RemotingCommand request, final RemotingCommand response);

}
