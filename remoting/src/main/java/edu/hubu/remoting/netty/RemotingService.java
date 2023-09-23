package edu.hubu.remoting.netty;

import edu.hubu.remoting.netty.handler.NettyRequestProcessor;
import edu.hubu.remoting.netty.handler.RpcHook;

import java.util.concurrent.ExecutorService;

/**
 * @author: sugar
 * @date: 2023/5/22
 * @description:
 */
public interface RemotingService {
    void start();

    void registerRpcHook(RpcHook rpcHook);
}
