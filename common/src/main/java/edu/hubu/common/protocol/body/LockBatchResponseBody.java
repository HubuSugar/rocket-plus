package edu.hubu.common.protocol.body;

import edu.hubu.common.message.MessageQueue;
import edu.hubu.remoting.netty.protocol.RemotingSerialize;

import java.util.Set;

/**
 * @author: sugar
 * @date: 2023/11/4
 * @description:
 */
public class LockBatchResponseBody extends RemotingSerialize {

    private Set<MessageQueue> lockedMessageQueue;

    public Set<MessageQueue> getLockedMessageQueue() {
        return lockedMessageQueue;
    }

    public void setLockedMessageQueue(Set<MessageQueue> lockedMessageQueue) {
        this.lockedMessageQueue = lockedMessageQueue;
    }
}
