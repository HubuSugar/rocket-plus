package edu.hubu.remoting.netty;

import edu.hubu.remoting.netty.exception.RemotingSendRequestException;
import edu.hubu.remoting.netty.exception.RemotingTimeoutException;
import edu.hubu.remoting.netty.handler.*;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;

/**
 * @author: sugar
 * @date: 2023/5/17
 * @description:
 */
@Slf4j
public abstract class NettyRemotingAbstract {

    protected final Semaphore semaphoreOneway;
    protected final Semaphore semaphoreAsync;

    protected Pair<NettyRequestProcessor, ExecutorService> defaultProcessor;

    protected final ConcurrentHashMap<Integer, ResponseFuture> responseTable = new ConcurrentHashMap<>(256);
    protected final HashMap<Integer, Pair<NettyRequestProcessor, ExecutorService>> processTable = new HashMap<>(64);

    public NettyRemotingAbstract(final int semaphoreOneway, final int semaphoreAsync) {
        this.semaphoreOneway = new Semaphore(semaphoreOneway, true);
        this.semaphoreAsync = new Semaphore(semaphoreAsync, true);
    }

    public void dispatchCommand(ChannelHandlerContext context, RemotingCommand command) throws Exception{
        if(command == null) return;
        switch (command.getCommandType()){
            case REQUEST:
                // log.info("【Request】command: {}", command);
                processRequestCommand(context, command);
                break;
            case RESPONSE:
                // log.info("【Response】command: {}", command);
                processResponseCommand(context, command);
                break;
            default:
                log.error("unknown cmd");
        }
    }


    public void processRequestCommand(final ChannelHandlerContext ctx, final RemotingCommand cmd){
        final Pair<NettyRequestProcessor, ExecutorService> matched = processTable.get(cmd.getCode());
        final Pair<NettyRequestProcessor, ExecutorService> pair = matched == null ? defaultProcessor : matched;
        final int opaque = cmd.getOpaque();

        if(pair == null) {
            log.error("request code unsupported");
            RemotingCommand response = RemotingCommand.createResponseCommand(ResponseCode.UNSUPPORTED_REQUEST_CODE, "unsupported request code");
            response.setOpaque(opaque);
            ctx.writeAndFlush(response);
            return;
        }
        Runnable r = new Runnable(){
            @Override
            public void run() {
                try{
                    ResponseInvokeCallback callback = new ResponseInvokeCallback(){
                        @Override
                        public void callback(RemotingCommand response) throws Exception {
                            if(!cmd.isOnewayType()){
                                if(response != null){
                                    response.setOpaque(opaque);
                                    response.markResponseType();
                                    try{
                                        ctx.writeAndFlush(response);
                                    }catch (Throwable e){
                                        log.error("request success, but response failed");
                                    }
                                }else{
                                }
                            }
                        }
                    };

                    if(pair.getKey() instanceof AsyncNettyRequestProcessor){
                        AsyncNettyRequestProcessor processor = (AsyncNettyRequestProcessor) pair.getKey();
                        processor.processRequestAsync(ctx, cmd, callback);
                    }else{
                        RemotingCommand response = pair.getKey().processRequest(ctx, cmd);
                        callback.callback(response);
                    }
                }catch (Throwable e){
                    log.error("process request exception", e);
                    if(!cmd.isOnewayType()){
                        RemotingCommand response = RemotingCommand.createResponseCommand(ResponseCode.SYSTEM_ERROR, e.getMessage());
                        response.setOpaque(opaque);
                        ctx.writeAndFlush(response);
                    }
                }

            }
        };

        //判断是否被限流
        if(pair.getKey().rejectRequest()){
            log.error("system busy");
            RemotingCommand response = RemotingCommand.createResponseCommand(ResponseCode.SYSTEM_BUSY, "system busy");
            ctx.writeAndFlush(response);
            return;
        }

        try{
            final RequestTask requestTask = new RequestTask(r, ctx.channel(), cmd);
            pair.getValue().submit(requestTask);
        }catch (RejectedExecutionException e){
            if(!cmd.isOnewayType()){
                RemotingCommand response = RemotingCommand.createResponseCommand(ResponseCode.SYSTEM_BUSY, "system busy start flow control for a while");
                response.setOpaque(opaque);
                ctx.writeAndFlush(response);
            }
        }

    }

    public void processResponseCommand(final ChannelHandlerContext ctx, final RemotingCommand cmd){
        final int opaque = cmd.getOpaque();
        final ResponseFuture responseFuture = responseTable.get(opaque);
        if(responseFuture != null){
            responseFuture.setResponseCommand(cmd);
            responseTable.remove(opaque);
            if(responseFuture.getCallback() != null){
                executeInvokeCallback(responseFuture);
            }else {
                responseFuture.putResponse(cmd);
                responseFuture.release();
            }
        }
    }

    public RemotingCommand invokeSyncImpl(final Channel channel, final RemotingCommand request, final long timeout) throws RemotingSendRequestException, InterruptedException, RemotingTimeoutException {
        int opaque = request.getOpaque();
        try{
            final ResponseFuture responseFuture = new ResponseFuture(opaque, channel, null, timeout);
            this.responseTable.put(opaque, responseFuture);
            channel.writeAndFlush(request).addListener(new ChannelFutureListener() {
                @Override
                public void operationComplete(ChannelFuture future) throws Exception {
                    if(future.isSuccess()){
                        responseFuture.setSendRequestOk(true);
                        return;
                    }else{
                        responseFuture.setSendRequestOk(false);
                    }
                    responseTable.remove(opaque);
                    responseFuture.setCause(future.cause());
                    responseFuture.putResponse(null);
                }
            });

            RemotingCommand response = responseFuture.waitResponse(timeout);
            if(response == null){
                if(responseFuture.isSendRequestOk()){
                    throw new RemotingTimeoutException(RemotingHelper.parseChannel2RemoteAddress(channel), timeout, responseFuture.getCause());
                }else{
                    throw new RemotingSendRequestException(RemotingHelper.parseChannel2RemoteAddress(channel), responseFuture.getCause());
                }
            }
            return response;
        }finally {
            responseTable.remove(opaque);
        }
    }

    public void invokeAsyncImpl(final Channel channel, final RemotingCommand request, final InvokeCallback callback){

    }

    private void executeInvokeCallback(final ResponseFuture responseFuture){

    }
}
