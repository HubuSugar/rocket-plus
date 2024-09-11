package edu.hubu.client.instance;

import edu.hubu.remoting.netty.handler.RpcHook;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author: sugar
 * @date: 2023/5/26
 * @description:
 */
@Slf4j
public class MQClientManager {

    private static final MQClientManager instance = new MQClientManager();
    private final AtomicInteger factoryIndexGenerator = new AtomicInteger();
    //<clientId, Instance>
    private ConcurrentHashMap<String, MQClientInstance> factoryTable = new ConcurrentHashMap<>();

    private MQClientManager(){

    }

    public static MQClientManager getInstance(){
        return instance;
    }

    public MQClientInstance getOrCreateInstance(final ClientConfig clientConfig){
        return getOrCreateInstance(clientConfig, null);
    }

    public MQClientInstance getOrCreateInstance(final ClientConfig clientConfig, RpcHook rpcHook){
        String clientId = clientConfig.buildMQClientId();
        MQClientInstance clientInstance = factoryTable.get(clientId);
        if(clientInstance == null){
            clientInstance = new MQClientInstance(clientConfig.cloneClientConfig(), factoryIndexGenerator.getAndIncrement(), clientId, rpcHook);
            MQClientInstance prev = factoryTable.putIfAbsent(clientId, clientInstance);
            if(prev != null){
              clientInstance = prev;
              log.warn("Returned the previous MQClientInstance for clientId: {}", clientId);
            }else{
                log.info("create new MQClientInstance for the clientId: {}", clientId);
            }
        }
        return clientInstance;
    }

}
