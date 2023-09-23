package edu.hubu.common.protocol.body;

import edu.hubu.remoting.netty.protocol.RemotingSerialize;

import java.util.ArrayList;
import java.util.List;

/**
 * @author: sugar
 * @date: 2023/6/12
 * @description:
 */
public class RegisterBrokerBody extends RemotingSerialize {
    private TopicConfigSerializeWrapper topicConfigSerializeWrapper = new TopicConfigSerializeWrapper();
    private List<String> filterServerList = new ArrayList<>();

    public byte[] encode(boolean compressed){
        if(!compressed){
           return super.encode();
        }
        return null;
    }

    public static RegisterBrokerBody decode(byte[] data, boolean compressed){
        if(!compressed){
            return RegisterBrokerBody.decode(data, RegisterBrokerBody.class);
        }
        return null;
    }

    public TopicConfigSerializeWrapper getTopicConfigSerializeWrapper() {
        return topicConfigSerializeWrapper;
    }

    public void setTopicConfigSerializeWrapper(TopicConfigSerializeWrapper topicConfigSerializeWrapper) {
        this.topicConfigSerializeWrapper = topicConfigSerializeWrapper;
    }

    public List<String> getFilterServerList() {
        return filterServerList;
    }

    public void setFilterServerList(List<String> filterServerList) {
        this.filterServerList = filterServerList;
    }
}
