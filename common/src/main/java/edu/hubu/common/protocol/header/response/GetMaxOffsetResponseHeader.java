package edu.hubu.common.protocol.header.response;

import edu.hubu.remoting.netty.CustomCommandHeader;

/**
 * @author: sugar
 * @date: 2023/11/5
 * @description:
 */
public class GetMaxOffsetResponseHeader implements CustomCommandHeader {

    private long offset;

    public long getOffset() {
        return offset;
    }

    public void setOffset(long offset) {
        this.offset = offset;
    }

    @Override
    public boolean checkFields() {
        return false;
    }
}
