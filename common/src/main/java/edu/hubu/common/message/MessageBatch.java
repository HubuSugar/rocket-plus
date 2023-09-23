package edu.hubu.common.message;

import java.util.Iterator;
import java.util.List;

/**
 * @author: sugar
 * @date: 2023/8/19
 * @description:
 */
public class MessageBatch extends Message implements Iterable<Message>{

    private List<Message> messages;

    @Override
    public Iterator<Message> iterator() {
        return messages.iterator();
    }

    public List<Message> getMessages() {
        return messages;
    }

    public void setMessages(List<Message> messages) {
        this.messages = messages;
    }
}
