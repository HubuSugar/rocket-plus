package edu.hubu.broker.starter;

import edu.hubu.remoting.netty.NettyRemotingClient;
import edu.hubu.remoting.netty.codec.NettyDecoder;
import edu.hubu.remoting.netty.codec.NettyEncoder;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import lombok.extern.slf4j.Slf4j;

/**
 * @author: sugar
 * @date: 2023/6/5
 * @description:
 */
@Slf4j
public class TestConnect {

    public static void main(String[] args) throws InterruptedException {
        Bootstrap bootstrap = new Bootstrap().group(new NioEventLoopGroup(1))
                .channel(NioSocketChannel.class)
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 3000)
                .option(ChannelOption.TCP_NODELAY, true)
                .option(ChannelOption.SO_KEEPALIVE, false)
                .option(ChannelOption.SO_SNDBUF, 10240)
                .option(ChannelOption.SO_RCVBUF, 10240)
                .handler(new ChannelInitializer<NioSocketChannel>() {
                    @Override
                    protected void initChannel(NioSocketChannel channel) throws Exception {
                        ChannelPipeline pipeline = channel.pipeline();
                        pipeline.addLast(
                                new NettyEncoder(),
                                new NettyDecoder()
                        );
                    }
                });

        ChannelFuture future = bootstrap.connect("127.0.0.1",10911).sync();
        future.addListener(new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture future) throws Exception {
                log.info("success");
                future.channel().writeAndFlush("hello");
            }
        });
    }
}
