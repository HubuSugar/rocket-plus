package edu.hubu.client.producer;

import edu.hubu.client.instance.MQClientInstance;

/**
 * @author: sugar
 * @date: 2024/8/3
 * @description:
 */
public class ClientRemotingProcessor {

    private final MQClientInstance mqClientInstance;

    public ClientRemotingProcessor(MQClientInstance mqClientInstance) {
        this.mqClientInstance = mqClientInstance;
    }
}
