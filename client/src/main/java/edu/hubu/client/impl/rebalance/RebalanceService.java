package edu.hubu.client.impl.rebalance;

import edu.hubu.client.instance.MQClientInstance;
import edu.hubu.common.ServiceThread;
import lombok.extern.slf4j.Slf4j;

/**
 * @author: sugar
 * @date: 2023/10/29
 * @description:
 */
@Slf4j
public class RebalanceService extends ServiceThread {

    private static final long waitInterval = Long.parseLong(System.getProperty("rocketmq.client.rebalance.waitInterval", "20000"));
    private final MQClientInstance mqClientInstance;

    public RebalanceService(MQClientInstance mqClientInstance) {
        this.mqClientInstance = mqClientInstance;
    }

    @Override
    public String getServiceName() {
        return RebalanceService.class.getSimpleName();
    }

    @Override
    public void run() {
        log.info("rebalance service started");

        while (isStopped()){
            this.waitForRunning(waitInterval);
            this.mqClientInstance.doRebalance();
        }

        log.info("rebalance service stopped");
    }
}
