package edu.hubu.common.protocol.header.request;

import edu.hubu.remoting.netty.CustomCommandHeader;
import edu.hubu.remoting.netty.annotation.NotNull;

/**
 * @author: sugar
 * @date: 2023/11/3
 * @description:
 */
public class GetConsumerIdListByGroupRequestHeader implements CustomCommandHeader {

    @NotNull
    private String consumerGroup;

    @Override
    public boolean checkFields() {
        return false;
    }

    public String getConsumerGroup() {
        return consumerGroup;
    }

    public void setConsumerGroup(String consumerGroup) {
        this.consumerGroup = consumerGroup;
    }
}
