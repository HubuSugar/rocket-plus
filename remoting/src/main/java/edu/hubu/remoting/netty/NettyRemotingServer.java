package edu.hubu.remoting.netty;

import edu.hubu.remoting.netty.codec.NettyDecoder;
import edu.hubu.remoting.netty.codec.NettyEncoder;
import edu.hubu.remoting.netty.common.RemotingHelper;
import edu.hubu.remoting.netty.common.RemotingUtil;
import edu.hubu.remoting.netty.handler.NettyEvent;
import edu.hubu.remoting.netty.handler.NettyEventType;
import edu.hubu.remoting.netty.handler.NettyRequestProcessor;
import edu.hubu.remoting.netty.common.Pair;
import edu.hubu.remoting.netty.handler.RpcHook;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.*;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import lombok.extern.slf4j.Slf4j;

import java.net.InetSocketAddress;
import java.util.Timer;
import java.util.TimerTask;
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

    private final Timer timer = new Timer("ServerHouseKeepingService", true);
    private DefaultEventExecutorGroup defaultEventExecutorGroup;

    private int port = 0;
    private ServerHandler serverHandler;
    private NettyEncoder nettyEncoder;
    private NettyConnectManageHandler connectManageHandler;

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
        this.defaultEventExecutorGroup = new DefaultEventExecutorGroup(
                nettyServerConfig.getServerWorkThreads(),
                new ThreadFactory() {
                    private final AtomicInteger threadIndex = new AtomicInteger(0);

                    @Override
                    public Thread newThread(Runnable r) {
                        return new Thread(r, "NettyServerCodecThreads_" + this.threadIndex.incrementAndGet());
                    }
                });

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
                        pipeline.addLast(
                                defaultEventExecutorGroup,
                                nettyEncoder,
                                new NettyDecoder(),
                                new IdleStateHandler(0, 0, nettyServerConfig.getServerChannelMaxIdleTimeSeconds()),
                                serverHandler
                        );

                    }
                });
        if(nettyServerConfig.isServerByteBufEnable()){
            bootstrap.childOption(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT);
        }

        try {
            ChannelFuture sync = bootstrap.bind().sync();
            InetSocketAddress socketAddress = (InetSocketAddress) sync.channel().localAddress();
            this.port = socketAddress.getPort();
        } catch (InterruptedException e) {
            log.error("exception", e);
            throw new RuntimeException("this.serverBootstrap.bind().sync() InterruptedException", e);
        }

        if(this.channelEventListener != null){
            this.nettyEventExecutor.start();
        }

        this.timer.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                try{
                    NettyRemotingServer.this.scanResponseTable();
                }catch (Exception e){
                    log.error("scanResponseTable exception", e);
                }
            }
        }, 1000 * 3, 1000);
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

    @Override
    public ChannelEventListener getChannelEventListener() {
        return channelEventListener;
    }

    @Override
    public ExecutorService getCallbackExecutor() {
        return this.publicExecutor;
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

    @ChannelHandler.Sharable
    class NettyConnectManageHandler extends ChannelDuplexHandler{

        @Override
        public void channelRegistered(ChannelHandlerContext ctx) throws Exception {
            final String remoteAddress = RemotingHelper.parseChannel2RemoteAddress(ctx.channel());
            log.info("NETTY SERVER PIPELINE: channelRegister {}", remoteAddress);
            super.channelRegistered(ctx);
        }

        @Override
        public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
            final String remoteAddress = RemotingHelper.parseChannel2RemoteAddress(ctx.channel());
            log.info("NETTY SERVER PIPELINE: channelUnregistered {}", remoteAddress);
            super.channelUnregistered(ctx);
        }

        @Override
        public void channelActive(ChannelHandlerContext ctx) throws Exception {
            final String remoteAddress = RemotingHelper.parseChannel2RemoteAddress(ctx.channel());
            log.info("NETTY SERVER PIPELINE: channelActive {}", remoteAddress);
            super.channelActive(ctx);

            if(NettyRemotingServer.this.channelEventListener != null){
                NettyRemotingServer.this.putNettyEvent(new NettyEvent(NettyEventType.CONNECT, remoteAddress, ctx.channel()));
            }
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) throws Exception {
            final String remoteAddress = RemotingHelper.parseChannel2RemoteAddress(ctx.channel());
            log.info("NETTY SERVER PIPELINE: channelInactive {}", remoteAddress);
            super.channelInactive(ctx);

            if(NettyRemotingServer.this.channelEventListener != null){
                NettyRemotingServer.this.putNettyEvent(new NettyEvent(NettyEventType.CLOSE, remoteAddress, ctx.channel()));
            }
        }

        @Override
        public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
            if (evt instanceof IdleStateEvent) {
                IdleStateEvent event = (IdleStateEvent) evt;
                if (event.state().equals(IdleState.ALL_IDLE)) {
                    final String remoteAddress = RemotingHelper.parseChannel2RemoteAddress(ctx.channel());
                    log.warn("NETTY SERVER PIPELINE: IDLE exception [{}]", remoteAddress);
                    RemotingUtil.closeChannel(ctx.channel());
                    if (NettyRemotingServer.this.channelEventListener != null) {
                        NettyRemotingServer.this
                                .putNettyEvent(new NettyEvent(NettyEventType.IDLE, remoteAddress, ctx.channel()));
                    }
                }
            }

            ctx.fireUserEventTriggered(evt);
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
            final String remoteAddress = RemotingHelper.parseChannel2RemoteAddress(ctx.channel());
            log.warn("NETTY SERVER PIPELINE: exceptionCaught {}", remoteAddress);
            log.warn("NETTY SERVER PIPELINE: exceptionCaught exception.", cause);

            if (NettyRemotingServer.this.channelEventListener != null) {
                NettyRemotingServer.this.putNettyEvent(new NettyEvent(NettyEventType.EXCEPTION, remoteAddress, ctx.channel()));
            }

            RemotingUtil.closeChannel(ctx.channel());
        }
    }
}
