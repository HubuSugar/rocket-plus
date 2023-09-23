package edu.hubu.remoting.netty;


import edu.hubu.remoting.netty.exception.RemotingConnectException;
import edu.hubu.remoting.netty.exception.RemotingSendRequestException;
import edu.hubu.remoting.netty.exception.RemotingTimeoutException;

import java.util.List;

/**
 * @author: sugar
 * @date: 2023/5/27
 * @description:
 */
public interface RemotingClient extends RemotingService{

    void updateNameSrvAddress(List<String> strings);

    List<String> getNameSrvList();

    RemotingCommand invokeSync(final String address, final RemotingCommand request, final long timeout)
            throws InterruptedException, RemotingConnectException, RemotingTimeoutException, RemotingSendRequestException;

    void invokeAsync(final String address,final RemotingCommand request, final InvokeCallback callback);

    void invokeOneway(final String address, final RemotingCommand request, final long timeoutMillis);
}
