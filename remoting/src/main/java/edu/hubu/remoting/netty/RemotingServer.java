package edu.hubu.remoting.netty;

import edu.hubu.remoting.netty.handler.NettyRequestProcessor;

import java.util.concurrent.ExecutorService;

/**
 * @author: sugar
 * @date: 2023/5/27
 * @description:
 */
public interface RemotingServer extends RemotingService{

    void registerProcessor(Integer requestCode, NettyRequestProcessor processor, ExecutorService executorService);

    void registerDefaultProcessor(NettyRequestProcessor processor, ExecutorService executorService);
}
