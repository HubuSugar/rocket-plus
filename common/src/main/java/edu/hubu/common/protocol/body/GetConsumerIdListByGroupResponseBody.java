package edu.hubu.common.protocol.body;

import edu.hubu.remoting.netty.protocol.RemotingSerialize;

import java.util.List;

/**
 * @author: sugar
 * @date: 2023/11/3
 * @description:
 */
public class GetConsumerIdListByGroupResponseBody extends RemotingSerialize {

    private List<String> consumerIdList;

    public List<String> getConsumerIdList() {
        return consumerIdList;
    }

    public void setConsumerIdList(List<String> consumerIdList) {
        this.consumerIdList = consumerIdList;
    }
}
