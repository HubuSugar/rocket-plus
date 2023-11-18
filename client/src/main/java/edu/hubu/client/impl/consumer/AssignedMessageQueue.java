package edu.hubu.client.impl.consumer;

import edu.hubu.client.impl.rebalance.RebalanceImpl;
import edu.hubu.common.message.MessageQueue;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author: sugar
 * @date: 2023/11/1
 * @description:
 */
public class AssignedMessageQueue {

    private RebalanceImpl rebalanceImpl;
    private final ConcurrentHashMap<MessageQueue, MessageQueueState> assignedQueueState;

    public AssignedMessageQueue() {
        this.assignedQueueState = new ConcurrentHashMap<>();
    }

    public void setRebalanceImpl(RebalanceImpl rebalanceImpl) {
        this.rebalanceImpl = rebalanceImpl;
    }

    /**
     * 调整分配到的队列
     * @param topic
     * @param assigned
     */
    public void updateAssignedMessageQueue(String topic, Set<MessageQueue> assigned) {
        synchronized (this.assignedQueueState){
            Iterator<Map.Entry<MessageQueue, MessageQueueState>> it = this.assignedQueueState.entrySet().iterator();
            while (it.hasNext()){
                Map.Entry<MessageQueue, MessageQueueState> next = it.next();
                if(next.getKey().getTopic().equals(topic)){
                    if(!assigned.contains(next.getKey())){
                        next.getValue().getProcessQueue().setDropped(true);
                        it.remove();
                    }
                }
            }
            addAssignedMessageQueue(assigned);
        }
    }

    private void addAssignedMessageQueue(Collection<MessageQueue> assigned) {
        for (MessageQueue messageQueue : assigned) {
            if(!assignedQueueState.containsKey(messageQueue)){
                MessageQueueState messageQueueState;
                if(rebalanceImpl != null && rebalanceImpl.getProcessQueueTable().get(messageQueue) != null){
                    messageQueueState = new MessageQueueState(messageQueue, rebalanceImpl.getProcessQueueTable().get(messageQueue));
                }else{
                    ProcessQueue processQueue = new ProcessQueue();
                    messageQueueState = new MessageQueueState(messageQueue, processQueue);
                }
                assignedQueueState.put(messageQueue, messageQueueState);
            }
        }
    }


    public Set<MessageQueue> getAssignedMessageQueues(){
        return this.assignedQueueState.keySet();
    }

    public boolean isPause(MessageQueue messageQueue) {
        MessageQueueState messageQueueState = this.assignedQueueState.get(messageQueue);
        if(messageQueueState != null){
            return messageQueueState.isPaused();
        }
        return true;
    }

    public ProcessQueue getProcessQueue(MessageQueue messageQueue) {
        MessageQueueState messageQueueState = this.assignedQueueState.get(messageQueue);
        if(messageQueueState != null){
            return messageQueueState.getProcessQueue();
        }
        return null;
    }

    public long getSeekOffset(MessageQueue messageQueue) {
        MessageQueueState messageQueueState = this.assignedQueueState.get(messageQueue);
        if(messageQueueState != null){
            return messageQueueState.getSeekOffset();
        }
        return -1;
    }

    public void setSeekOffset(MessageQueue messageQueue, int offset) {
        MessageQueueState messageQueueState = this.assignedQueueState.get(messageQueue);
        if(messageQueueState != null){
            messageQueueState.setSeekOffset(offset);
        }
    }

    public void updateConsumeOffset(MessageQueue messageQueue, long offset) {
        MessageQueueState messageQueueState = this.assignedQueueState.get(messageQueue);
        if(messageQueueState != null){
            messageQueueState.setConsumeOffset(offset);
        }
    }

    public long getPullOffset(MessageQueue messageQueue) {
        MessageQueueState messageQueueState = this.assignedQueueState.get(messageQueue);
        if(messageQueueState != null){
            return messageQueueState.getPullOffset();
        }
        return -1;
    }


    static class MessageQueueState {
        private MessageQueue messageQueue;
        private ProcessQueue processQueue;
        private volatile boolean paused = false;
        private volatile long pullOffset = -1;
        private volatile long consumeOffset = -1;
        private volatile long seekOffset = -1;

        public MessageQueueState(MessageQueue messageQueue, ProcessQueue processQueue) {
            this.messageQueue = messageQueue;
            this.processQueue = processQueue;
        }

        public MessageQueue getMessageQueue() {
            return messageQueue;
        }

        public void setMessageQueue(MessageQueue messageQueue) {
            this.messageQueue = messageQueue;
        }

        public ProcessQueue getProcessQueue() {
            return processQueue;
        }

        public void setProcessQueue(ProcessQueue processQueue) {
            this.processQueue = processQueue;
        }

        public boolean isPaused() {
            return paused;
        }

        public void setPaused(boolean paused) {
            this.paused = paused;
        }

        public long getPullOffset() {
            return pullOffset;
        }

        public void setPullOffset(long pullOffset) {
            this.pullOffset = pullOffset;
        }

        public long getConsumeOffset() {
            return consumeOffset;
        }

        public void setConsumeOffset(long consumeOffset) {
            this.consumeOffset = consumeOffset;
        }

        public long getSeekOffset() {
            return seekOffset;
        }

        public void setSeekOffset(long seekOffset) {
            this.seekOffset = seekOffset;
        }
    }


}
