package edu.hubu.remoting.netty.handler;

import edu.hubu.remoting.netty.RemotingCommand;

/**
 * @author: sugar
 * @date: 2023/5/19
 * @description:
 */
public interface ResponseInvokeCallback {
    void callback(RemotingCommand response) throws Exception;
}
