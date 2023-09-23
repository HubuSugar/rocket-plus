package edu.hubu.common.protocol.route;

import edu.hubu.remoting.netty.protocol.RemotingSerialize;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * @author: sugar
 * @date: 2023/6/2
 * @description:
 */
public class TopicRouteData extends RemotingSerialize {
    //顺序topic配置 e.g brokerName1:queueNum1;brokerName2:queueNum2;brokerName3:queueNum3
    private String orderTopicConf;
    private List<QueueData> queueData;
    private List<BrokerData> brokerData;
    //filterServerTable
    private HashMap<String, List<String>> filterServerTable = new HashMap<>();

    public TopicRouteData cloneTopicRouteInfo(){
        TopicRouteData topicRouteData = new TopicRouteData();
        topicRouteData.setQueueData(new ArrayList<>());
        topicRouteData.setBrokerData(new ArrayList<>());
        if(queueData != null){
            topicRouteData.getQueueData().addAll(this.queueData);
        }
        if(brokerData !=null){
            topicRouteData.getBrokerData().addAll(this.brokerData);
        }
        return topicRouteData;
    }


    public String getOrderTopicConf() {
        return orderTopicConf;
    }

    public void setOrderTopicConf(String orderTopicConf) {
        this.orderTopicConf = orderTopicConf;
    }

    public List<QueueData> getQueueData() {
        return queueData;
    }

    public void setQueueData(List<QueueData> queueData) {
        this.queueData = queueData;
    }

    public List<BrokerData> getBrokerData() {
        return brokerData;
    }

    public void setBrokerData(List<BrokerData> brokerData) {
        this.brokerData = brokerData;
    }

    public HashMap<String, List<String>> getFilterServerTable() {
        return filterServerTable;
    }

    public void setFilterServerTable(HashMap<String, List<String>> filterServerTable) {
        this.filterServerTable = filterServerTable;
    }
}
