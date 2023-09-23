package edu.hubu.namesrv;

import edu.hubu.remoting.netty.NettyServerConfig;

/**
 * @author: sugar
 * @date: 2023/6/6
 * @description:
 */
public class NamesrvStarter {

    public static void main(String[] args) {
        start(createNamesrvController(args));
    }

    public static NamesrvController createNamesrvController(String[] args){
        NettyServerConfig nettyServerConfig = new NettyServerConfig();
        nettyServerConfig.setListenPort(9877);

        NamesrvConfig namesrvConfig = new NamesrvConfig();
        return new NamesrvController(namesrvConfig, nettyServerConfig);
    }

    public static void start(NamesrvController namesrvController){
        namesrvController.initialize();
        namesrvController.start();
    }
}
