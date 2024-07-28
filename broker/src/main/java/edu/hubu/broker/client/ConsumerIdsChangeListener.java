package edu.hubu.broker.client;

/**
 * @author: sugar
 * @date: 2024/7/27
 * @description:
 */
public interface ConsumerIdsChangeListener {
    void handle(ConsumerGroupEvent consumerGroupEvent, String group, Object... args);
}
