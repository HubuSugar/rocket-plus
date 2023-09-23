package edu.hubu.store.listen;

import java.util.Map;

/**
 * @author: sugar
 * @date: 2023/7/15
 * @description:
 */
public interface MessageArrivingListener {

    void arriving(String topic, int queueId, long logicOffset, long tagsCode, long msgStoreTime, byte[] filterBitMap, Map<String, String> properties);
}
