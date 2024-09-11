package edu.hubu.remoting.netty.codec;

import edu.hubu.remoting.netty.RemotingCommand;
import edu.hubu.remoting.netty.common.RemotingUtil;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

import java.nio.ByteBuffer;

/**
 * @author: sugar
 * @date: 2023/5/17
 * @description:
 */
@ChannelHandler.Sharable
public class NettyEncoder extends MessageToByteEncoder<RemotingCommand> {

    @Override
    protected void encode(ChannelHandlerContext ctx, RemotingCommand command, ByteBuf out) throws Exception {
        try{
            ByteBuffer headData = command.encodeHeader();
            out.writeBytes(headData);

            byte[] body = command.getBody();
            if(body != null){
                out.writeBytes(body);
            }
        }catch (Exception e){
            RemotingUtil.closeChannel(ctx.channel());
        }
    }
}
