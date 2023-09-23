package edu.hubu.common.protocol.header.request;

import edu.hubu.remoting.netty.CustomCommandHeader;

/**
 * @author: sugar
 * @date: 2023/6/4
 * @description:
 */
public class GetTopicRouteInfoHeader implements CustomCommandHeader {
    private String topic;

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    @Override
    public boolean checkFields() {
        return false;
    }
}
