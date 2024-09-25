package edu.hubu.common.protocol.route;

import lombok.EqualsAndHashCode;

/**
 * @author: sugar
 * @date: 2023/6/2
 * @description:
 */
@EqualsAndHashCode
public class QueueData implements Comparable<QueueData>{
    private String brokerName;
    private int readQueueNums;
    private int writeQueueNums;
    private int perm;
    private int topicSynFlag;

    public String getBrokerName() {
        return brokerName;
    }

    public void setBrokerName(String brokerName) {
        this.brokerName = brokerName;
    }

    public int getReadQueueNums() {
        return readQueueNums;
    }

    public void setReadQueueNums(int readQueueNums) {
        this.readQueueNums = readQueueNums;
    }

    public int getWriteQueueNums() {
        return writeQueueNums;
    }

    public void setWriteQueueNums(int writeQueueNums) {
        this.writeQueueNums = writeQueueNums;
    }

    public int getPerm() {
        return perm;
    }

    public void setPerm(int perm) {
        this.perm = perm;
    }

    public int getTopicSynFlag() {
        return topicSynFlag;
    }

    public void setTopicSynFlag(int topicSynFlag) {
        this.topicSynFlag = topicSynFlag;
    }

    @Override
    public String toString() {
        return "QueueData{" +
                "brokerName='" + brokerName + '\'' +
                ", readQueueNums=" + readQueueNums +
                ", writeQueueNums=" + writeQueueNums +
                ", perm=" + perm +
                ", topicSynFlag=" + topicSynFlag +
                '}';
    }

    @Override
    public int compareTo(QueueData o) {
        return this.brokerName.compareTo(o.getBrokerName());
    }
}
