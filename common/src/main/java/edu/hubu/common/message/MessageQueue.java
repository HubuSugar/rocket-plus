package edu.hubu.common.message;

import lombok.EqualsAndHashCode;

/**
 * @author: sugar
 * @date: 2023/5/29
 * @description:
 */
@EqualsAndHashCode
public class MessageQueue implements Comparable<MessageQueue>{
    private String topic;
    private String brokerName;
    private int queueId;

    public MessageQueue(String topic, String brokerName, int queueId) {
        this.topic = topic;
        this.brokerName = brokerName;
        this.queueId = queueId;
    }

    public String getBrokerName() {
        return brokerName;
    }

    public void setBrokerName(String brokerName) {
        this.brokerName = brokerName;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public int getQueueId() {
        return queueId;
    }

    public void setQueueId(int queueId) {
        this.queueId = queueId;
    }

    @Override
    public int compareTo(MessageQueue o) {
        int result = this.topic.compareTo(o.topic);
        if(result != 0){
            return result;
        }

        result = this.brokerName.compareTo(o.brokerName);
        if(result != 0){
            return result;
        }
        return this.queueId - o.queueId;
    }

    @Override
    public String toString() {
        return "MessageQueue{" +
                "topic='" + topic + '\'' +
                ", brokerName='" + brokerName + '\'' +
                ", queueId=" + queueId +
                '}';
    }
}
