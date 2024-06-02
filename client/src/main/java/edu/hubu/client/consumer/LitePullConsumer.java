package edu.hubu.client.consumer;

import edu.hubu.client.exception.MQClientException;
import edu.hubu.common.message.MessageExt;

import java.util.List;

/**
 * @author: sugar
 * @date: 2023/10/28
 * @description:
 */
public interface LitePullConsumer {

    void start() throws MQClientException;

    void shutdown();

    void subscribe(String topic, String subExpression) throws MQClientException;

    void subscribe(String topic, MessageSelector messageSelector);

    List<MessageExt> poll();

    List<MessageExt> poll(long timeoutMillis);

    boolean isAutoCommit();

}
