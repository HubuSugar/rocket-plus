package edu.hubu.client.impl;

import edu.hubu.client.common.ThreadLocalIndex;
import edu.hubu.common.message.MessageQueue;
import edu.hubu.common.protocol.route.QueueData;
import edu.hubu.common.protocol.route.TopicRouteData;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author: sugar
 * @date: 2023/5/29
 * @description:
 */
public class TopicPublishInfo {
    private boolean orderTopic = false;
    private boolean haveTopicRouteInfo = false;
    private List<MessageQueue> messageQueues = new ArrayList<>();
    private TopicRouteData topicRouteData;
    private final ThreadLocalIndex sendWhichQueue = new ThreadLocalIndex();

    public boolean ok(){
        return  messageQueues != null && !messageQueues.isEmpty();
    }

    public MessageQueue selectOneMessageQueue(){
        int index = sendWhichQueue.getAndIncrement();
        int pos = Math.abs(index) % this.messageQueues.size();
        if(pos < 0) pos = 0;
        return this.messageQueues.get(pos);
    }

    public MessageQueue selectOneMessageQueue(String lastBrokerName) {
        if(lastBrokerName != null){
            int index = this.sendWhichQueue.getAndIncrement();
            for (int i = 0; i < this.messageQueues.size(); i++) {
                int pos = Math.abs(index++) % this.messageQueues.size();
                if(pos < 0) pos =0;
                MessageQueue mq = messageQueues.get(pos);
                if(!mq.getBrokerName().equals(lastBrokerName)){
                    return mq;
                }
            }
        }
        return selectOneMessageQueue();
    }

    public int findQueueNumsByBrokerName(final String brokerName) {
        for (int i = 0; i < topicRouteData.getQueueData().size(); i++) {
            QueueData queueData = topicRouteData.getQueueData().get(i);
            if (queueData.getBrokerName().equals(brokerName)) {
                return queueData.getWriteQueueNums();
            }
        }
        return -1;
    }

    public boolean isOrderTopic() {
        return orderTopic;
    }

    public void setOrderTopic(boolean orderTopic) {
        this.orderTopic = orderTopic;
    }

    public boolean isHaveTopicRouteInfo() {
        return haveTopicRouteInfo;
    }

    public void setHaveTopicRouteInfo(boolean haveTopicRouteInfo) {
        this.haveTopicRouteInfo = haveTopicRouteInfo;
    }

    public List<MessageQueue> getMessageQueues() {
        return messageQueues;
    }

    public void setMessageQueues(List<MessageQueue> messageQueues) {
        this.messageQueues = messageQueues;
    }

    public TopicRouteData getTopicRouteData() {
        return topicRouteData;
    }

    public void setTopicRouteData(TopicRouteData topicRouteData) {
        this.topicRouteData = topicRouteData;
    }

    public ThreadLocalIndex getSendWhichQueue() {
        return sendWhichQueue;
    }

}
