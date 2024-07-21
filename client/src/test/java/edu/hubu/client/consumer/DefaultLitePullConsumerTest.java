package edu.hubu.client.consumer;

import edu.hubu.client.exception.MQClientException;
import edu.hubu.common.consumer.ConsumeFromWhere;
import edu.hubu.common.message.MessageExt;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;

import java.util.List;

/**
 * @author: sugar
 * @date: 2023/10/29
 * @description:
 */
@Slf4j
public class DefaultLitePullConsumerTest {

    @Test
    public void test(){
        System.out.println(111);
    }

    @Test
    public void consumeMessage() throws MQClientException {

        DefaultLitePullConsumer consumer = new DefaultLitePullConsumer("testConsumer");
        consumer.setNameServer("127.0.0.1:9877");
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        consumer.subscribe("TopicTest", "*");
        consumer.start();

        try{
            while (true){
                List<MessageExt> poll = consumer.poll();
                System.out.println(poll);
            }

        }finally {
            consumer.shutdown();
        }


    }
}
