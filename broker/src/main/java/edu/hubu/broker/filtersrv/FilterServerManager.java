package edu.hubu.broker.filtersrv;

import edu.hubu.broker.starter.BrokerController;

import java.util.ArrayList;
import java.util.List;

/**
 * @author: sugar
 * @date: 2023/7/2
 * @description:
 */
public class FilterServerManager {
    private final BrokerController brokerController;

    public FilterServerManager(BrokerController brokerController) {
        this.brokerController = brokerController;
    }

    public List<String> buildNewFilterServerList(){
        return new ArrayList<>();
    }
}
