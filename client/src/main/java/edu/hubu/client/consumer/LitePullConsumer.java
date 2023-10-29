package edu.hubu.client.consumer;

import edu.hubu.client.exception.MQClientException;

/**
 * @author: sugar
 * @date: 2023/10/28
 * @description:
 */
public interface LitePullConsumer {

    void start() throws MQClientException;

    void shutdown();

    void subscribe(String topic, String subExpression);

    void subscribe(String topic, MessageSelector messageSelector);

}
