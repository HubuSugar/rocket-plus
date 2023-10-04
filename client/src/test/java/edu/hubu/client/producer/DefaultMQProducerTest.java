package edu.hubu.client.producer;

import edu.hubu.client.exception.MQClientException;
import edu.hubu.remoting.netty.exception.RemotingException;
import edu.hubu.common.exception.broker.MQBrokerException;
import edu.hubu.common.message.Message;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;

import java.nio.charset.StandardCharsets;

/**
 * @author: sugar
 * @date: 2023/5/25
 * @description:
 */
@Slf4j
public class DefaultMQProducerTest {

    @Test
    public void test(){
        System.out.println(111);
    }

    @Test
    public void sendMessage() throws RemotingException, InterruptedException, MQClientException, MQBrokerException {

        DefaultMQProducer mqProducer = new DefaultMQProducer("test group");
        mqProducer.setNameServer("127.0.0.1:9877");
        mqProducer.start();
        int successCount = 0, failCount = 0;
        for(int i = 0; i < 50; i++){
            Message message = new Message("test");
            String body = "the first message, this is my first day to learn rocket mq " + i;
            message.setBody(body.getBytes(StandardCharsets.UTF_8));
            SendResult sendResult = mqProducer.send(message);
            if(sendResult.getSendStatus() == SendStatus.SEND_OK){
                successCount++;
            }else{
                log.info("出现失败：i:{}, result:{}",i, sendResult);
                failCount++;
            }
            // log.info("sendResult: {}", sendResult);
        }
        System.out.println("successCount:" + successCount + " failCount:" + failCount);

    }
}
