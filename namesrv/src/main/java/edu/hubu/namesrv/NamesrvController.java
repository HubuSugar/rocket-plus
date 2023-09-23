package edu.hubu.namesrv;

import edu.hubu.namesrv.impl.KvConfigManager;
import edu.hubu.namesrv.impl.TopicRouteInfoManager;
import edu.hubu.namesrv.processor.DefaultRequestProcessor;
import edu.hubu.remoting.netty.NettyRemotingServer;
import edu.hubu.remoting.netty.NettyServerConfig;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author: sugar
 * @date: 2023/6/6
 * @description:
 */
public class NamesrvController {

    private final NamesrvConfig namesrvConfig;
    private final NettyServerConfig nettyServerConfig;
    private final TopicRouteInfoManager topicRouteInfoManager;
    private final KvConfigManager kvConfigManager;
    private NettyRemotingServer nettyRemotingServer;
    private ExecutorService remotingExecutor;


    public NamesrvController(NamesrvConfig namesrvConfig, NettyServerConfig nettyServerConfig) {
        this.nettyServerConfig = nettyServerConfig;
        this.namesrvConfig = namesrvConfig;
        this.topicRouteInfoManager = new TopicRouteInfoManager();
        this.kvConfigManager = new KvConfigManager();
    }

    public void initialize(){
        this.remotingExecutor = Executors.newFixedThreadPool(nettyServerConfig.getWorkThreadsNum(), new ThreadFactory() {
            private final AtomicInteger threadIndex = new AtomicInteger();
            @Override
            public Thread newThread(Runnable r) {
                return new Thread(r, String.format("namesrvExecutorThread_%d", threadIndex.getAndIncrement()));
            }
        });
        this.nettyRemotingServer = new NettyRemotingServer(nettyServerConfig);
        this.registerDefaultProcessor();
    }

    public void start(){
        this.nettyRemotingServer.start();
    }

    public void registerDefaultProcessor(){
        this.nettyRemotingServer.registerDefaultProcessor(new DefaultRequestProcessor(this), this.remotingExecutor);
    }

    public NamesrvConfig getNamesrvConfig() {
        return namesrvConfig;
    }

    public TopicRouteInfoManager getTopicRouteInfoManager() {
        return topicRouteInfoManager;
    }

    public KvConfigManager getKvConfigManager() {
        return kvConfigManager;
    }
}
