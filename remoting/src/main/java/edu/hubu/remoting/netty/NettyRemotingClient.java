package edu.hubu.remoting.netty;

import edu.hubu.remoting.netty.codec.NettyDecoder;
import edu.hubu.remoting.netty.codec.NettyEncoder;
import edu.hubu.remoting.netty.exception.RemotingConnectException;
import edu.hubu.remoting.netty.exception.RemotingSendRequestException;
import edu.hubu.remoting.netty.exception.RemotingTimeoutException;
import edu.hubu.remoting.netty.handler.RpcHook;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import lombok.extern.slf4j.Slf4j;

import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author: sugar
 * @date: 2023/5/27
 * @description:
 */
@Slf4j
public class NettyRemotingClient extends NettyRemotingAbstract implements RemotingClient {
    private static final long LOCK_TIMEOUT = 3 * 1000;
    private final Bootstrap bootstrap = new Bootstrap();
    private final EventLoopGroup eventLoopGroupWorker;
    private final NettyClientConfig nettyClientConfig;

    //nameSrv集群地址
    private final AtomicReference<List<String>> nameSrvAddressList = new AtomicReference<>();
    //选择的nameSrv节点地址
    private final AtomicReference<String> nameSrvAddressChosen = new AtomicReference<>();
    private final Lock nameSrvChannelLock = new ReentrantLock();

    private final AtomicInteger nameSrvIndex = new AtomicInteger(initIndex());
    private final ConcurrentMap<String, ChannelWrapper> channelTables = new ConcurrentHashMap<>();
    private final Lock channelTablesLock = new ReentrantLock();

    public NettyRemotingClient(final NettyClientConfig nettyClientConfig) {
        super(nettyClientConfig.getClientSemaphoreOneway(), nettyClientConfig.getClientSemaphoreAsync());
        this.nettyClientConfig = nettyClientConfig;
        this.eventLoopGroupWorker = new NioEventLoopGroup(1);
    }

    private static int initIndex(){
        Random random = new Random();
        return Math.abs(random.nextInt() % 999) % 999;
    }

    @Override
    public void start() {
        Bootstrap handler = this.bootstrap.group(this.eventLoopGroupWorker)
                .channel(NioSocketChannel.class)
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, this.nettyClientConfig.getClientConnectTimeout())
                .option(ChannelOption.TCP_NODELAY, true)
                .option(ChannelOption.SO_KEEPALIVE, false)
                .option(ChannelOption.SO_SNDBUF, this.nettyClientConfig.getClientSndBufSize())
                .option(ChannelOption.SO_RCVBUF, this.nettyClientConfig.getClientRcvBufSize())
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel channel) throws Exception {
                        ChannelPipeline pipeline = channel.pipeline();
                        pipeline.addLast(
                                new NettyEncoder(),
                                new NettyDecoder(),
                                new NettyClientHandler()
                                );
                    }
                });
    }

    @Override
    public void updateNameSrvAddress(List<String> address) {
        List<String> old = this.nameSrvAddressList.get();
        if(address != null && !address.isEmpty()){
            boolean updated = false;

            if(old == null){
                updated = true;
            }else if(old.size() != address.size()){
                updated = true;
            }else{
                for (String s : address) {
                    if (!old.contains(s)) {
                        updated = true;
                        break;
                    }
                }
            }

            if(updated){
                Collections.shuffle(address);
                this.nameSrvAddressList.set(address);
                log.warn("name srv address has updated");

                if(!address.contains(this.nameSrvAddressChosen.get())){
                    this.nameSrvAddressChosen.set(null);
                }
            }
        }
    }

    @Override
    public List<String> getNameSrvList() {
        return this.nameSrvAddressList.get();
    }

    @Override
    public RemotingCommand invokeSync(final String address,final RemotingCommand request, long timeout) throws InterruptedException, RemotingConnectException,
            RemotingTimeoutException, RemotingSendRequestException {
        final long beginTimestamp = System.currentTimeMillis();
        final Channel channel = this.getAndCreateChannel(address);
        if(channel != null && channel.isActive()){
           try {
               long costTime = System.currentTimeMillis() - beginTimestamp;
               if(costTime > timeout){
                   throw new RemotingTimeoutException("invoke sync timeout: {}ms", timeout);
               }
               return invokeSyncImpl(channel, request, timeout - costTime);
           }catch (RemotingSendRequestException e){
               log.error("invoke sync remoting send request exception", e);
               this.closeChannel(address, channel);
               throw e;
           }catch (RemotingTimeoutException e){
               if(nettyClientConfig.isCloseChannelWhenSocketTimeout()){
                   this.closeChannel(address, channel);
               }
               throw e;
           }
        }else{
            this.closeChannel(address, channel);
            throw new RemotingConnectException(address);
        }
    }


    @Override
    public void invokeAsync(String address, RemotingCommand request, InvokeCallback callback) {

    }

    @Override
    public void registerRpcHook(RpcHook rpcHook) {

    }

    @Override
    public void invokeOneway(String address, RemotingCommand request, long timeoutMillis) {

    }

    public void closeChannel(final String address, final Channel channel)  {
        if(channel == null) return;
        String remoteAddress = address == null ? RemotingHelper.parseChannel2RemoteAddress(channel) : address;

        try{
            if (channelTablesLock.tryLock(LOCK_TIMEOUT, TimeUnit.MILLISECONDS)) {
                try{
                    boolean removeItemFromTable = true;
                    ChannelWrapper cw = channelTables.get(remoteAddress);
                    if(cw == null){
                        log.info("close channel, channel has been removed from already");
                        removeItemFromTable = false;
                    }else if(cw.getChannel() != channel){
                        log.info("close channel, channel has been created again");
                        removeItemFromTable = false;
                    }

                    if(removeItemFromTable){
                        this.channelTables.remove(remoteAddress);
                    }
                    RemotingUtil.closeChannel(channel);
                }finally {
                    channelTablesLock.unlock();
                }
            }else {
                log.warn("close channel, try to lock but timeout: {}ms", LOCK_TIMEOUT);
            }
        }catch (InterruptedException e){
            log.error("close channel, interruptedException ", e);
        }
    }

    /**
     * 1、地址为空，从namesrv获取channel
     * 2、从缓存map中获取channel
     * 3、直接创建channel
     * @param address the available address of the brokers
     * @return the result of socket channel
     */
    private Channel getAndCreateChannel(final String address) throws InterruptedException, RemotingConnectException {
        if(null == address){
            return this.getAndCreateNameSrvChannel();
        }

        ChannelWrapper cw = this.channelTables.get(address);
        if(cw != null && cw.isChannelOk()){
            return cw.getChannel();
        }

        return createChannel(address);
    }

    private Channel getAndCreateNameSrvChannel() throws InterruptedException, RemotingConnectException {
        String address = nameSrvAddressChosen.get();
        if (address != null) {
            ChannelWrapper cw = this.channelTables.get(address);
            if (cw != null && cw.isChannelOk()) {
                return cw.getChannel();
            }
        }

        final List<String> addresses = nameSrvAddressList.get();

        if (nameSrvChannelLock.tryLock(LOCK_TIMEOUT, TimeUnit.MILLISECONDS)) {
            try {
                address = nameSrvAddressChosen.get();
                if(address != null){
                    ChannelWrapper cw = this.channelTables.get(address);
                    if (cw != null && cw.isChannelOk()) {
                        return cw.getChannel();
                    }
                }

                if(addresses != null && !addresses.isEmpty()){
                    for(int i = 0; i < addresses.size();i++){
                        int index = nameSrvIndex.incrementAndGet();
                        index = Math.abs(index) % addresses.size();
                        String newAddress = addresses.get(index);
                        nameSrvAddressChosen.set(newAddress);
                        log.warn("a new nameSrv address was chosen, old: {}, new:{}, nameSrvIndex:{}", address, newAddress, nameSrvIndex);
                        Channel channel = createChannel(newAddress);
                        if(channel != null){
                            return channel;
                        }
                    }
                    throw new RemotingConnectException(addresses.toString());
                }
            } finally {
                nameSrvChannelLock.unlock();
            }
        }else{
            log.warn("getAndNameSrvChannel try to lock name srv timeout, {}ms", LOCK_TIMEOUT);
        }
        return null;
    }

    private Channel createChannel(String address) throws InterruptedException {
        ChannelWrapper cw = this.channelTables.get(address);
        if(cw != null && cw.isChannelOk()){
            return cw.getChannel();
        }

        if (channelTablesLock.tryLock(LOCK_TIMEOUT, TimeUnit.MILLISECONDS)) {
            try{
                boolean createNewConnection;
                cw = this.channelTables.get(address);
                if(cw != null){
                    //需要按断channel的状态，因为map中保存的channel可能是刚建立连接的还在初始化
                    if(cw.isChannelOk()){
                        return cw.getChannel();
                    }else if(!cw.getChannelFuture().isDone()){ //isDone()判断任务是否执行完成
                        createNewConnection = false;
                    }else {
                        this.channelTables.remove(address);
                        createNewConnection = true;
                    }
                }else {
                    createNewConnection = true;
                }

                if(createNewConnection){
                    ChannelFuture channelFuture = this.bootstrap.connect(RemotingHelper.string2SocketAddress(address));
                    log.info("create channel: begin to connect remote host[{}] asynchronously", address);
                    cw = new ChannelWrapper(channelFuture);
                    this.channelTables.put(address, cw);
                }
            }catch (Exception e){
                log.error("create channel exception", e);
            } finally{
                channelTablesLock.unlock();
            }
        }else {
            log.warn("createChannel try to lock name srv but timeout:{}ms", LOCK_TIMEOUT);
        }

        if(cw != null){
            ChannelFuture channelFuture = cw.getChannelFuture();
            //等待channel初始化完成
            if (channelFuture.awaitUninterruptibly(this.nettyClientConfig.getClientConnectTimeout())) {
                if(cw.isChannelOk()){
                    log.info("create channel: connect remote host[{}] success, channel future: {}", address, channelFuture);
                    return cw.getChannel();
                }else{
                    log.warn("create channel, connect remote host [{}] failed, channelFuture:{}, cause:{}", address, channelFuture, channelFuture.cause());
                }
            }else{
                log.warn("create channel, connect remote host[{}] timeout: {}ms ", address, this.nettyClientConfig.getClientConnectTimeout());
            }
        }
        return null;
    }

    static class ChannelWrapper{
        private final ChannelFuture channelFuture;

        public ChannelWrapper(ChannelFuture channelFuture) {
            this.channelFuture = channelFuture;
        }

        public boolean isChannelOk(){
            return this.channelFuture.channel() != null && this.channelFuture.channel().isActive();
        }

        public boolean isChannelWritable(){
            return this.channelFuture.channel().isWritable();
        }

        public Channel getChannel(){
            return this.channelFuture.channel();
        }

        public ChannelFuture getChannelFuture() {
            return channelFuture;
        }

    }

    class NettyClientHandler extends SimpleChannelInboundHandler<RemotingCommand> {
        @Override
        protected void channelRead0(ChannelHandlerContext ctx, RemotingCommand command) throws Exception {
            dispatchCommand(ctx, command);
        }
    }
}
