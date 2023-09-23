package edu.hubu.client.instance;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author: sugar
 * @date: 2023/5/26
 * @description:
 */
public class MQClientManager {

    private static final MQClientManager instance = new MQClientManager();
    private final AtomicInteger factoryIndexGenerator = new AtomicInteger();
    private ConcurrentHashMap<String, MQClientInstance> factoryTable = new ConcurrentHashMap<>();

    public static MQClientManager getInstance(){
        return instance;
    }

    public MQClientInstance getOrCreateInstance(final ClientConfig clientConfig){
        String clientId = clientConfig.buildMQClientId();
        MQClientInstance clientInstance = factoryTable.get(clientId);
        if(clientInstance == null){
            clientInstance = new MQClientInstance(clientConfig, factoryIndexGenerator.getAndIncrement(), clientId);
            MQClientInstance prev = factoryTable.putIfAbsent(clientId, clientInstance);
            if(prev != null){
              clientInstance = prev;
            }
        }
        return clientInstance;
    }

}
