package edu.hubu.client.consumer;

import edu.hubu.client.exception.MQClientException;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;

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
        consumer.start();

    }
}
