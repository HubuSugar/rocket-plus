package edu.hubu.broker.client;

import edu.hubu.broker.starter.BrokerController;
import edu.hubu.remoting.netty.ChannelEventListener;
import io.netty.channel.Channel;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

/**
 * @author: sugar
 * @date: 2024/9/13
 * @description:
 */
@Slf4j
public class ClientHousekeepingService implements ChannelEventListener {

    private final BrokerController brokerController;

    private final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
        @Override
        public Thread newThread(Runnable r) {
            return new Thread(r, "ClientHousekeepingService");
        }
    });


    public ClientHousekeepingService(BrokerController brokerController) {
        this.brokerController = brokerController;
    }

    public void start(){
        this.scheduledExecutorService.scheduleAtFixedRate(() -> {
            try {
                ClientHousekeepingService.this.scanExceptionChannel();
            }catch (Throwable e){
                log.error("error occurred when scan not active client channels ", e);
            }
        }, 1000 * 10, 1000 * 10, TimeUnit.MILLISECONDS);
    }

    public void scanExceptionChannel(){
        this.brokerController.getProducerManager().scanNotActiveChannel();
        this.brokerController.getConsumerManager().scanNotActiveChannel();
        //filterServer not active channels
    }

    public void shutdown(){
        this.scheduledExecutorService.shutdown();
    }

    @Override
    public void onChannelConnect(String remoteAddress, Channel channel) {

    }

    @Override
    public void onChannelClose(String remoteAddress, Channel channel) {
        this.brokerController.getProducerManager().doChannelCloseEvent(remoteAddress, channel);
        this.brokerController.getConsumerManager().doChannelCloseEvent(remoteAddress, channel);
    }

    @Override
    public void onChannelException(String remoteAddress, Channel channel) {
        this.brokerController.getProducerManager().doChannelCloseEvent(remoteAddress, channel);
        this.brokerController.getConsumerManager().doChannelCloseEvent(remoteAddress, channel);
    }

    @Override
    public void onChannelIdle(String remoteAddress, Channel channel) {
        this.brokerController.getProducerManager().doChannelCloseEvent(remoteAddress, channel);
        this.brokerController.getConsumerManager().doChannelCloseEvent(remoteAddress, channel);
    }
}
