package edu.hubu.remoting.netty;

import edu.hubu.remoting.netty.codec.NettyDecoder;
import edu.hubu.remoting.netty.codec.NettyEncoder;
import edu.hubu.remoting.netty.handler.NettyRequestProcessor;
import edu.hubu.remoting.netty.handler.Pair;
import edu.hubu.remoting.netty.handler.RpcHook;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.*;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.ServerSocketChannel;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import lombok.extern.slf4j.Slf4j;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author: sugar
 * @date: 2023/5/15
 * @description:
 */
@Slf4j
public class NettyRemotingServer extends NettyRemotingAbstract implements RemotingServer{

    private final ServerBootstrap serverBootstrap;
    private final EventLoopGroup eventLoopGroupBoss;
    private final EventLoopGroup eventLoopGroupWorker;
    private final NettyServerConfig nettyServerConfig;

    private final ExecutorService publicExecutor;
    private final ChannelEventListener channelEventListener;

    private ServerHandler serverHandler;
    private NettyEncoder nettyEncoder;
    private int port = 0;

    public NettyRemotingServer(final NettyServerConfig nettyServerConfig) {
        this(nettyServerConfig, null);
    }

    public NettyRemotingServer(final NettyServerConfig nettyServerConfig, final ChannelEventListener channelEventListener) {
        super(nettyServerConfig.getSemaphoreOneway(), nettyServerConfig.getSemaphoreAsync());
        this.serverBootstrap = new ServerBootstrap();
        this.nettyServerConfig = nettyServerConfig;
        this.channelEventListener = channelEventListener;

        this.publicExecutor = Executors.newFixedThreadPool(nettyServerConfig.getServerPublicThreads(), new ThreadFactory() {
            private final AtomicInteger threadIndex = new AtomicInteger(0);
            @Override
            public Thread newThread(Runnable r) {
                return new Thread(r, String.format("serverPublicExecutor_%d", threadIndex.getAndIncrement()));
            }
        });

        if(useEpoll()){
            this.eventLoopGroupBoss = new EpollEventLoopGroup(1, new ThreadFactory() {
                private final AtomicInteger threadIndex = new AtomicInteger(0);
                @Override
                public Thread newThread(Runnable r) {
                    return new Thread(r, String.format("eventLoopBoss_%d", threadIndex.incrementAndGet()));
                }
            });
            this.eventLoopGroupWorker = new EpollEventLoopGroup(nettyServerConfig.getWorkThreadsNum(), new ThreadFactory() {
                private final AtomicInteger threadIndex = new AtomicInteger(0);
                private final int total = nettyServerConfig.getWorkThreadsNum();
                @Override
                public Thread newThread(Runnable r) {
                    return new Thread(r, String.format("eventLoopWorker_%d_%d", threadIndex.incrementAndGet(), total));
                }
            });
        }else{
            this.eventLoopGroupBoss = new NioEventLoopGroup(1, new ThreadFactory() {
                private final AtomicInteger threadIndex = new AtomicInteger(0);
                @Override
                public Thread newThread(Runnable r) {
                    return new Thread(r, String.format("eventLoopBoss_%d", threadIndex.incrementAndGet()));
                }
            });
            this.eventLoopGroupWorker = new NioEventLoopGroup(nettyServerConfig.getWorkThreadsNum(), new ThreadFactory() {
                private final AtomicInteger threadIndex = new AtomicInteger(0);
                private final int total = nettyServerConfig.getWorkThreadsNum();
                @Override
                public Thread newThread(Runnable r) {
                    return new Thread(r, String.format("eventLoopWorker_%d_%d", threadIndex.incrementAndGet(), total));
                }
            });
        }

    }

    @Override
    public void start(){
        prepareSharableHandlers();

        ServerBootstrap bootstrap = this.serverBootstrap.group(eventLoopGroupBoss, eventLoopGroupWorker)
                .channel(useEpoll() ? EpollServerSocketChannel.class : NioServerSocketChannel.class)
                .option(ChannelOption.SO_BACKLOG, 1024)
                .option(ChannelOption.SO_REUSEADDR, true)
                .option(ChannelOption.SO_KEEPALIVE, false)
                .childOption(ChannelOption.TCP_NODELAY, true)
                .childOption(ChannelOption.SO_SNDBUF, nettyServerConfig.getSndBufSize())
                .childOption(ChannelOption.SO_RCVBUF, nettyServerConfig.getRevBufSize())
                .localAddress(new InetSocketAddress(this.nettyServerConfig.getListenPort()))
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel channel) throws Exception {
                        ChannelPipeline pipeline = channel.pipeline();
                        pipeline.addLast(nettyEncoder, new NettyDecoder(), serverHandler);

                    }
                });
        if(nettyServerConfig.isServerByteBufEnable()){
            bootstrap.childOption(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT);
        }

        try {
            ChannelFuture sync = bootstrap.bind().sync();
            SocketAddress socketAddress = sync.channel().localAddress();
            this.port =((InetSocketAddress)socketAddress).getPort();
        } catch (InterruptedException e) {
            log.error("exception", e);
        }

    }

    private void prepareSharableHandlers(){
        this.nettyEncoder = new NettyEncoder();
        this.serverHandler = new ServerHandler();
    }

    @Override
    public void registerProcessor(Integer requestCode, NettyRequestProcessor processor, ExecutorService executorService){
        ExecutorService executor = executorService;
        if(executor == null){
            executor = publicExecutor;
        }
        processTable.put(requestCode, new Pair<>(processor, executor));
    }


    @Override
    public void registerDefaultProcessor(NettyRequestProcessor processor, ExecutorService executorService) {
        this.defaultProcessor = new Pair<>(processor, executorService);
    }

    @Override
    public void registerRpcHook(RpcHook rpcHook) {

    }

    private boolean useEpoll(){
        return RemotingUtil.isLinuxPlatform();
    }


    public ServerBootstrap getServerBootstrap() {
        return serverBootstrap;
    }

    public EventLoopGroup getEventLoopGroupBoss() {
        return eventLoopGroupBoss;
    }

    public EventLoopGroup getEventLoopGroupWorker() {
        return eventLoopGroupWorker;
    }

    public NettyServerConfig getNettyServerConfig() {
        return nettyServerConfig;
    }

    public ChannelEventListener getChannelEventListener() {
        return channelEventListener;
    }

    public int getPort() {
        return port;
    }

    @ChannelHandler.Sharable
    class ServerHandler extends SimpleChannelInboundHandler<RemotingCommand>{
        @Override
        protected void channelRead0(ChannelHandlerContext channelHandlerContext, RemotingCommand remotingCommand) throws Exception {
            dispatchCommand(channelHandlerContext, remotingCommand);
        }
    }
}
