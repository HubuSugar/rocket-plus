package edu.hubu.common.protocol.heartbeat;

import edu.hubu.remoting.netty.protocol.RemotingSerialize;

import java.util.HashSet;
import java.util.Set;

/**
 * @author: sugar
 * @date: 2024/7/21
 * @description:
 */
public class HeartbeatData extends RemotingSerialize {
    private String clientId;
    private Set<ProducerData> produceData = new HashSet<>();
    private Set<ConsumerData> consumeData = new HashSet<>();

    public String getClientId() {
        return clientId;
    }

    public void setClientId(String clientId) {
        this.clientId = clientId;
    }

    public Set<ProducerData> getProduceData() {
        return produceData;
    }

    public void setProduceData(Set<ProducerData> produceData) {
        this.produceData = produceData;
    }

    public Set<ConsumerData> getConsumeData() {
        return consumeData;
    }

    public void setConsumeData(Set<ConsumerData> consumeData) {
        this.consumeData = consumeData;
    }
}
