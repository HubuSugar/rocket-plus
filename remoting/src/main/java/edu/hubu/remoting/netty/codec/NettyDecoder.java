package edu.hubu.remoting.netty.codec;

import edu.hubu.remoting.netty.RemotingCommand;
import edu.hubu.remoting.netty.common.RemotingUtil;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;

import java.nio.ByteBuffer;

/**
 * @author: sugar
 * @date: 2023/5/17
 * @description:
 */
public class NettyDecoder extends LengthFieldBasedFrameDecoder {

    private static final int FRAME_MAX_LENGTH = 16777216;

    public NettyDecoder() {
        super(FRAME_MAX_LENGTH, 0,4,0, 4);
    }

    @Override
    protected Object decode(ChannelHandlerContext ctx, ByteBuf in) throws Exception {

        ByteBuf frame = null;
        try{
            frame = (ByteBuf)super.decode(ctx, in);
            if(frame == null){
                return null;
            }
            ByteBuffer byteBuffer = frame.nioBuffer();
            return RemotingCommand.decode(byteBuffer);
        }catch (Exception e){
            RemotingUtil.closeChannel(ctx.channel());
        }finally {
            if(frame != null){
                frame.release();
            }
        }

        return null;
    }
}
