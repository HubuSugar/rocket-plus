package edu.hubu.common.protocol.header.response;

import edu.hubu.remoting.netty.CustomCommandHeader;

/**
 * @author: sugar
 * @date: 2023/11/4
 * @description:
 */
public class GetConsumerIdListByGroupResponseHeader implements CustomCommandHeader {
    @Override
    public boolean checkFields() {
        return false;
    }
}
