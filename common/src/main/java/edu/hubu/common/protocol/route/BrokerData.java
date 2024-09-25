package edu.hubu.common.protocol.route;

import edu.hubu.common.utils.MixAll;
import lombok.EqualsAndHashCode;

import java.util.*;

/**
 * @author: sugar
 * @date: 2023/6/2
 * @description:
 */
@EqualsAndHashCode
public class BrokerData implements Comparable<BrokerData>{
    private String cluster;
    private String brokerName;
    //<brokerId, brokerAddress>
    private HashMap<Long, String> brokerAddrTable = new HashMap<>();
    private final Random random = new Random();


    public String selectBrokerAddr() {
        String brokerAddr = brokerAddrTable.get(MixAll.MASTER_ID);
        if(brokerAddr == null){
            List<String> brokerAddrs = new ArrayList<>(brokerAddrTable.values());
            return brokerAddrs.get(random.nextInt(brokerAddrs.size()));
        }
        return brokerAddr;
    }

    public String getCluster() {
        return cluster;
    }

    public BrokerData setCluster(String cluster) {
        this.cluster = cluster;
        return this;
    }

    public String getBrokerName() {
        return brokerName;
    }

    public BrokerData setBrokerName(String brokerName) {
        this.brokerName = brokerName;
        return this;
    }

    public HashMap<Long, String> getBrokerAddrTable() {
        return brokerAddrTable;
    }

    public BrokerData setBrokerAddrTable(HashMap<Long, String> brokerAddrTable) {
        this.brokerAddrTable = brokerAddrTable;
        return this;
    }



    @Override
    public String toString() {
        return "BrokerData{" +
                "cluster='" + cluster + '\'' +
                ", brokerName='" + brokerName + '\'' +
                ", brokerAddrTable=" + brokerAddrTable +
                ", random=" + random +
                '}';
    }

    @Override
    public int compareTo(BrokerData o) {
        return this.brokerName.compareTo(o.getBrokerName());
    }

}
