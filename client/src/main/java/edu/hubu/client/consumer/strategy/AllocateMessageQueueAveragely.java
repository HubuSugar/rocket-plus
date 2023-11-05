package edu.hubu.client.consumer.strategy;

import edu.hubu.common.message.MessageQueue;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;

/**
 * @author: sugar
 * @date: 2023/10/28
 * @description:
 */
@Slf4j
public class AllocateMessageQueueAveragely implements AllocateMessageQueueStrategy {

    @Override
    public List<MessageQueue> allocate(String consumerGroup, String clientId, List<MessageQueue> mqSet, List<String> cidAll) {
        if(clientId == null || clientId.length() == 0){
            throw new IllegalArgumentException("clientId can not be null or empty.");
        }
        if(mqSet == null || mqSet.isEmpty()){
            throw new IllegalArgumentException("mqSet can not be null or empty");
        }
        if(cidAll == null || cidAll.isEmpty()){
            throw new IllegalArgumentException("cidAll can not be null or empty");
        }
        List<MessageQueue> result = new ArrayList<>();
        if(!cidAll.contains(clientId)){
            log.warn("consumerGroup: {}, clientId :{} not in cidAll: {}", consumerGroup, clientId, cidAll);
            return result;
        }
        int index = cidAll.indexOf(clientId);
        //队列数能不能被消费者平均分掉
        int mod = mqSet.size() % cidAll.size();
        //队列数 小于 消费者数那么每个消费者分到的队列为1
        int avgSize = mqSet.size() < cidAll.size() ? 1 : (mod > 0 && index < mod ? mqSet.size() / cidAll.size() + 1 : mqSet.size() / cidAll.size());
        int startIndex = (mod > 0 && index < mod) ? index * avgSize : index * avgSize + mod;
        int range = Math.min(avgSize, mqSet.size() - startIndex);

        for (int i = 0; i < range; i++) {
            result.add(mqSet.get((startIndex + i) % mqSet.size()));
        }

        return result;
    }

    @Override
    public String getName() {
        return AllocateMessageQueueStrategy.class.getSimpleName();
    }
}
