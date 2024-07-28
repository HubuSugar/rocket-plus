package edu.hubu.broker.client;

import edu.hubu.broker.starter.BrokerController;

/**
 * @author: sugar
 * @date: 2024/7/27
 * @description:
 */
public class DefaultConsumerIdsChangeListener implements ConsumerIdsChangeListener{

    private final BrokerController brokerController;

    public DefaultConsumerIdsChangeListener(final BrokerController brokerController) {
        this.brokerController = brokerController;
    }

    @Override
    public void handle(ConsumerGroupEvent event, String group, Object... args) {
        if(event == null){
            return;
        }
        switch (event){
            case CHANGED:
                break;
            case REGISTER:
                break;

        }

    }
}
