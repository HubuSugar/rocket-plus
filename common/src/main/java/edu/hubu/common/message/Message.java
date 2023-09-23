package edu.hubu.common.message;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * @author: sugar
 * @date: 2023/5/25
 * @description:
 */
public class Message implements Serializable {
    private static final long serialVersionUID = 8445773977080406428L;

    private String topic;
    //
    private int flag;
    //1、消息的重试消费次数
    private Map<String, String> properties;
    private byte[] body;
    private String transactionId;

    public Message() {
    }

    public Message(String topic) {
        this.topic = topic;

        this.setWaitStoreOk(true);
    }

    public Message(String topic, boolean waitStoreOk) {
        this.topic = topic;

        this.setWaitStoreOk(waitStoreOk);
    }

    public String getProperty(String key){
        if(properties == null) {
          this.properties = new HashMap<>();
        }
        return properties.get(key);
    }

    public String putProperty(String key, String value){
        if(properties == null){
            this.properties = new HashMap<>();
        }
        return this.properties.put(key, value);
    }

    public int getDelayTimeLevel(){
        String level = getProperty(MessageConst.PROPERTY_DELAY_LEVEL);
        if(level != null){
            return Integer.parseInt(level);
        }
        return 0;
    }

    public boolean isWaitStoreOk(){
        String property = this.getProperty(MessageConst.PROPERTY_WAIT_STORE_OK);
        return property == null || Boolean.parseBoolean(property);
    }

    public void setWaitStoreOk(boolean waitStoreOk){
        this.putProperty(MessageConst.PROPERTY_WAIT_STORE_OK, String.valueOf(waitStoreOk));
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public byte[] getBody() {
        return body;
    }

    public void setBody(byte[] body) {
        this.body = body;
    }

    public int getFlag() {
        return flag;
    }

    public void setFlag(int flag) {
        this.flag = flag;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public void setProperties(Map<String, String> properties) {
        this.properties = properties;
    }

    public String getTransactionId() {
        return transactionId;
    }

    public void setTransactionId(String transactionId) {
        this.transactionId = transactionId;
    }
}
