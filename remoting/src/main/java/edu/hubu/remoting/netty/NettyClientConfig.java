package edu.hubu.remoting.netty;

/**
 * @author: sugar
 * @date: 2023/5/27
 * @description:
 */
public class NettyClientConfig {
    private int clientSemaphoreOneway = 8;
    private int clientSemaphoreAsync = 8;
    private int clientConnectTimeout = 3 * 1000;
    private int clientSndBufSize = 16777216;
    private int clientRcvBufSize = 16777216;
    private boolean closeChannelWhenSocketTimeout = true;

    public int getClientSemaphoreOneway() {
        return clientSemaphoreOneway;
    }

    public void setClientSemaphoreOneway(int clientSemaphoreOneway) {
        this.clientSemaphoreOneway = clientSemaphoreOneway;
    }

    public int getClientSemaphoreAsync() {
        return clientSemaphoreAsync;
    }

    public void setClientSemaphoreAsync(int clientSemaphoreAsync) {
        this.clientSemaphoreAsync = clientSemaphoreAsync;
    }

    public int getClientConnectTimeout() {
        return clientConnectTimeout;
    }

    public void setClientConnectTimeout(int clientConnectTimeout) {
        this.clientConnectTimeout = clientConnectTimeout;
    }

    public int getClientSndBufSize() {
        return clientSndBufSize;
    }

    public void setClientSndBufSize(int clientSndBufSize) {
        this.clientSndBufSize = clientSndBufSize;
    }

    public int getClientRcvBufSize() {
        return clientRcvBufSize;
    }

    public void setClientRcvBufSize(int clientRcvBufSize) {
        this.clientRcvBufSize = clientRcvBufSize;
    }

    public boolean isCloseChannelWhenSocketTimeout() {
        return closeChannelWhenSocketTimeout;
    }

    public void setCloseChannelWhenSocketTimeout(boolean closeChannelWhenSocketTimeout) {
        this.closeChannelWhenSocketTimeout = closeChannelWhenSocketTimeout;
    }
}
