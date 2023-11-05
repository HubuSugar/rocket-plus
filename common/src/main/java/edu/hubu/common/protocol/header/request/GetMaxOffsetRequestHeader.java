package edu.hubu.common.protocol.header.request;

import edu.hubu.remoting.netty.CustomCommandHeader;

/**
 * @author: sugar
 * @date: 2023/11/5
 * @description:
 */
public class GetMaxOffsetRequestHeader implements CustomCommandHeader {

    private String topic;
    private Integer queueId;

    @Override
    public boolean checkFields() {
        return false;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public Integer getQueueId() {
        return queueId;
    }

    public void setQueueId(Integer queueId) {
        this.queueId = queueId;
    }
}
