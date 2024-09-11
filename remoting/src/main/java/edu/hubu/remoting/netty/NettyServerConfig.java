package edu.hubu.remoting.netty;

/**
 * @author: sugar
 * @date: 2023/5/16
 * @description:
 */
public class NettyServerConfig {

    private int listenPort = 8888;
    private int serverWorkThreads = 8;
    private int workThreadsNum = 8;
    private int sndBufSize = 10240;
    private int revBufSize = 10240;
    private boolean serverByteBufEnable = true;
    private int semaphoreOneway = 256;
    private int semaphoreAsync = 256;
    private int serverPublicThreads = 8;
    private int serverChannelMaxIdleTimeSeconds = 120;


    public int getListenPort() {
        return listenPort;
    }

    public void setListenPort(int listenPort) {
        this.listenPort = listenPort;
    }

    public int getWorkThreadsNum() {
        return workThreadsNum;
    }

    public void setWorkThreadsNum(int workThreadsNum) {
        this.workThreadsNum = workThreadsNum;
    }

    public int getSndBufSize() {
        return sndBufSize;
    }

    public void setSndBufSize(int sndBufSize) {
        this.sndBufSize = sndBufSize;
    }

    public int getRevBufSize() {
        return revBufSize;
    }

    public void setRevBufSize(int revBufSize) {
        this.revBufSize = revBufSize;
    }

    public boolean isServerByteBufEnable() {
        return serverByteBufEnable;
    }

    public void setServerByteBufEnable(boolean serverByteBufEnable) {
        this.serverByteBufEnable = serverByteBufEnable;
    }

    public int getSemaphoreOneway() {
        return semaphoreOneway;
    }

    public void setSemaphoreOneway(int semaphoreOneway) {
        this.semaphoreOneway = semaphoreOneway;
    }

    public int getSemaphoreAsync() {
        return semaphoreAsync;
    }

    public void setSemaphoreAsync(int semaphoreAsync) {
        this.semaphoreAsync = semaphoreAsync;
    }

    public int getServerPublicThreads() {
        return serverPublicThreads;
    }

    public void setServerPublicThreads(int serverPublicThreads) {
        this.serverPublicThreads = serverPublicThreads;
    }

    public int getServerWorkThreads() {
        return serverWorkThreads;
    }

    public void setServerWorkThreads(int serverWorkThreads) {
        this.serverWorkThreads = serverWorkThreads;
    }

    public int getServerChannelMaxIdleTimeSeconds() {
        return serverChannelMaxIdleTimeSeconds;
    }

    public void setServerChannelMaxIdleTimeSeconds(int serverChannelMaxIdleTimeSeconds) {
        this.serverChannelMaxIdleTimeSeconds = serverChannelMaxIdleTimeSeconds;
    }
}
