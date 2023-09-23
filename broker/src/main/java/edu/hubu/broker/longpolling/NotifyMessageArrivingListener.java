package edu.hubu.broker.longpolling;

import edu.hubu.store.listen.MessageArrivingListener;

import java.util.Map;

/**
 * @author: sugar
 * @date: 2023/7/15
 * @description:
 */
public class NotifyMessageArrivingListener implements MessageArrivingListener {
    private PullHoldService pullHoldService;

    public NotifyMessageArrivingListener(PullHoldService pullHoldService) {
        this.pullHoldService = pullHoldService;
    }

    @Override
    public void arriving(String topic, int queueId, long logicOffset, long tagsCode, long msgStoreTime, byte[] filterBitMap, Map<String, String> properties) {

    }
}
