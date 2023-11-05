package edu.hubu.broker.longpolling;

import edu.hubu.store.listen.MessageArrivingListener;

import java.util.Map;

/**
 * @author: sugar
 * @date: 2023/7/15
 * @description:
 */
public class NotifyMessageArrivingListener implements MessageArrivingListener {
    private PullRequestHoldService pullRequestHoldService;

    public NotifyMessageArrivingListener(PullRequestHoldService pullHoldService) {
        this.pullRequestHoldService = pullHoldService;
    }

    @Override
    public void arriving(String topic, int queueId, long logicOffset, long tagsCode, long msgStoreTime, byte[] filterBitMap, Map<String, String> properties) {

    }
}
