package edu.hubu.common.message;

/**
 * @author: sugar
 * @date: 2023/5/29
 * @description:
 */
public class MessageQueue {
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
    public String toString() {
        return "MessageQueue{" +
                "topic='" + topic + '\'' +
                ", brokerName='" + brokerName + '\'' +
                ", queueId=" + queueId +
                '}';
    }
}
