package edu.hubu.broker.pageCache;

import edu.hubu.store.GetMessageResult;
import io.netty.channel.FileRegion;
import io.netty.util.AbstractReferenceCounted;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.List;

/**
 * @author: sugar
 * @date: 2024/5/2
 * @description:
 */
public class ManyMessageTransfer extends AbstractReferenceCounted implements FileRegion {

    private final ByteBuffer byteBufferHeader;
    private final GetMessageResult getResult;
    private long transferred;

    public ManyMessageTransfer(ByteBuffer byteBufferHeader, GetMessageResult getResult) {
        this.byteBufferHeader = byteBufferHeader;
        this.getResult = getResult;
    }

    @Override
    public long position() {
        int position = byteBufferHeader.position();
        List<ByteBuffer> msgBufferList = this.getResult.getMsgBufferList();
        for (ByteBuffer bb : msgBufferList) {
            position += bb.position();
        }
        return position;
    }

    @Override
    public long transfered() {
        return this.transferred;
    }

    @Override
    public long count() {
        return 0;
    }

    @Override
    public long transferTo(WritableByteChannel writableByteChannel, long l) throws IOException {
        return 0;
    }



    @Override
    protected void deallocate() {

    }
}
