package edu.hubu.broker.starter;

import edu.hubu.common.BrokerConfig;
import edu.hubu.remoting.netty.NettyClientConfig;
import edu.hubu.remoting.netty.NettyServerConfig;
import edu.hubu.store.config.MessageStoreConfig;
import lombok.extern.slf4j.Slf4j;

/**
 * @author: sugar
 * @date: 2023/5/24
 * @description:
 */
@Slf4j
public class BrokerStarter {


    public static void main(String[] args) {
        start(createController(args));
    }

    public static BrokerController createController(String[] args){

        NettyClientConfig nettyClientConfig = new NettyClientConfig();

        NettyServerConfig nettyServerConfig = new NettyServerConfig();
        nettyServerConfig.setListenPort(10921);

        BrokerConfig brokerConfig = new BrokerConfig();

        MessageStoreConfig messageStoreConfig = new MessageStoreConfig();

        BrokerController brokerController = new BrokerController(brokerConfig, nettyServerConfig, nettyClientConfig, messageStoreConfig);
        brokerController.initialize();

        return brokerController;
    }

    public static BrokerController start(final BrokerController controller){
        try{
            controller.start();

            return controller;
        }catch (Exception e){
            log.error("start broker exception", e);
            System.exit(-1);
        }
        return null;
    }
}
