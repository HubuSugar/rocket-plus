package edu.hubu.broker.offset;

import edu.hubu.broker.starter.BrokerController;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * @author: sugar
 * @date: 2024/5/26
 * @description:
 */
@Slf4j
public class ConsumerOffsetManager {
    private static final String TOPIC_CONSUMER_SEPARATOR = "@";
    private ConcurrentMap<String, ConcurrentMap<Integer, Long>> offsetTable = new ConcurrentHashMap<>(512);

    private transient BrokerController brokerController;


    public ConsumerOffsetManager(BrokerController brokerController) {
        this.brokerController = brokerController;
    }

    public void commitOffset(final String clientHost, final String consumerGroup, final String topic, final int queueId, final long commitOffset) {
        String key = topic + TOPIC_CONSUMER_SEPARATOR + consumerGroup;
        this.commitOffset(clientHost, key, queueId, commitOffset);
    }

    public void commitOffset(final String clientHost, final String key, final int queueId, final long commitOffset) {
        ConcurrentMap<Integer, Long> map = this.offsetTable.get(key);
        if(map == null){
            map = new ConcurrentHashMap<>(32);
            map.put(queueId, commitOffset);
            this.offsetTable.put(key, map);
        }else{
            Long storeOffset = map.put(queueId, commitOffset);
            if(storeOffset != null && storeOffset > commitOffset){
                log.warn("[notify] update consumer offset less than store, clientHost={}, key={},queueId={},offset={},storeOffset={}", clientHost, key,queueId, commitOffset, storeOffset );
            }
        }
    }
}