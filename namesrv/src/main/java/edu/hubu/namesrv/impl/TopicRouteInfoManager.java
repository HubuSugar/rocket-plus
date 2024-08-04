package edu.hubu.namesrv.impl;

import edu.hubu.common.DataVersion;
import edu.hubu.common.TopicConfig;
import edu.hubu.common.protocol.body.TopicConfigSerializeWrapper;
import edu.hubu.common.protocol.result.RegisterBrokerResult;
import edu.hubu.common.protocol.route.BrokerData;
import edu.hubu.common.protocol.route.QueueData;
import edu.hubu.common.protocol.route.TopicRouteData;
import edu.hubu.common.utils.MixAll;
import edu.hubu.namesrv.routeInfo.BrokerLiveInfo;
import io.netty.channel.Channel;
import lombok.extern.slf4j.Slf4j;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * @author: sugar
 * @date: 2023/6/10
 * @description:
 */
@Slf4j
public class TopicRouteInfoManager {

    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    //<topic, QueueData>
    private final HashMap<String, List<QueueData>> topicQueueTable;
    //<brokerName, BrokerData>
    private final HashMap<String, BrokerData> brokerAddressTable;
    // <brokerAddress, filterServerAddress>
    private final HashMap<String, List<String>> filterServerTable;
    //<clusterName, Set<BrokerAddress>>
    private final HashMap<String, Set<String>> clusterTable;
    //<brokerAddress, BrokerLiveInfo>
    private final HashMap<String, BrokerLiveInfo> brokerLiveTable;


    public TopicRouteInfoManager() {
        this.topicQueueTable = new HashMap<>();
        this.brokerAddressTable = new HashMap<>();
        this.filterServerTable = new HashMap<>();
        this.clusterTable = new HashMap<>();
        this.brokerLiveTable = new HashMap<>();
    }

    /**
     * 根据topic查询topicRouteData
     * @param topic
     * @return
     */
    public TopicRouteData pickupRouteData(final String topic) {
        TopicRouteData topicRouteData = new TopicRouteData();
        boolean foundQueueData = false;
        boolean  foundBrokerData = false;
        Set<String> brokerNameSet = new HashSet<>();
        List<BrokerData> brokerDatas = new LinkedList<>();
        topicRouteData.setBrokerData(brokerDatas);

        HashMap<String, List<String>> filterServerList = new HashMap<>();
        topicRouteData.setFilterServerTable(filterServerList);

        try{
            try{
                this.lock.readLock().lockInterruptibly();
                List<QueueData> queueData = topicQueueTable.get(topic);
                if(queueData != null){
                    topicRouteData.setQueueData(queueData);
                    foundQueueData = true;

                    for (QueueData data : queueData) {
                        brokerNameSet.add(data.getBrokerName());
                    }

                    for (String brokerName : brokerNameSet) {
                        BrokerData brokerData = this.brokerAddressTable.get(brokerName);
                        if(brokerData != null){
                            BrokerData newBroker = new BrokerData().setCluster(brokerData.getCluster()).setBrokerName(brokerName).setBrokerAddrTable((HashMap<Long, String>) brokerData.getBrokerAddrTable().clone());
                            brokerDatas.add(newBroker);
                            foundBrokerData = true;
                            for (String brokerAddr : newBroker.getBrokerAddrTable().values()) {
                                List<String> filerServers = this.filterServerTable.get(brokerAddr);
                                filterServerList.put(brokerAddr, filerServers);
                            }
                        }
                    }
                }
            }finally {
                this.lock.readLock().unlock();
            }
        }catch (Exception e){
           log.error("name srv pick up topic route info exception", e);
        }

        if(foundBrokerData && foundQueueData){
            return topicRouteData;
        }

        return null;
    }

    /**
     * 用于broker向nameSrv注册
     * @param brokerName
     * @param brokerAddress
     * @param clusterName
     * @param brokerId
     * @param haServer
     * @param topicConfigSerializeWrapper
     * @param filterServerList
     * @param channel
     * @return
     */
    public RegisterBrokerResult registerAllBroker(String brokerName, String brokerAddress, String clusterName,
                                                  long brokerId, String haServer, TopicConfigSerializeWrapper topicConfigSerializeWrapper,
                                                  List<String> filterServerList, Channel channel) {
        RegisterBrokerResult result = new RegisterBrokerResult();
        try {
            try {
                this.lock.writeLock().lockInterruptibly();
                Set<String> allBrokers = this.clusterTable.get(clusterName);
                //不存在时，设置默认值
                if(allBrokers == null){
                    allBrokers = new HashSet<>();
                    this.clusterTable.put(clusterName, allBrokers);
                }
                allBrokers.add(brokerName);
                //是否首次注册
                boolean registerFirst = false;
                BrokerData brokerData = this.brokerAddressTable.get(brokerName);
                if(brokerData == null){
                    registerFirst = true;
                    brokerData = new BrokerData().setBrokerName(brokerName).setCluster(clusterName).setBrokerAddrTable(new HashMap<Long, String>());
                    this.brokerAddressTable.put(brokerName, brokerData);
                }
                Map<Long, String> brokerAddrTable = brokerData.getBrokerAddrTable();
                Iterator<Map.Entry<Long, String>> iterator = brokerAddrTable.entrySet().iterator();
                //将从节点选举为主节点时，首先移除从节点地址<1, IP:PORT> 然后添加主节点的地址<0, IP:PORT>
                //必须保证一对地址IP:PORT 只对应一条记录
                //Switch slave to master: first remove <1, IP:PORT> in namesrv, then add <0, IP:PORT>
                //The same IP:PORT must only have one record in brokerAddrTable
                while(iterator.hasNext()){
                    Map.Entry<Long, String> next = iterator.next();
                    if(brokerAddress != null && brokerAddress.equals(next.getValue()) && brokerId != next.getKey()){
                        iterator.remove();
                    }
                }
                String oldAddr = brokerData.getBrokerAddrTable().put(brokerId, brokerAddress);
                registerFirst = registerFirst || (oldAddr == null);
                if(null != topicConfigSerializeWrapper && brokerId == MixAll.MASTER_ID){
                    //判断broker的topicConfig是否改变， 如果发生改变还要同时更新QueueData
                    if(this.isBrokerTopicConfigChanged(brokerAddress, topicConfigSerializeWrapper.getDataVersion()) || registerFirst){
                        ConcurrentHashMap<String, TopicConfig> topicConfigTable = topicConfigSerializeWrapper.getTopicConfigTable();
                        if(topicConfigTable != null){
                            for (Map.Entry<String, TopicConfig> configEntry : topicConfigTable.entrySet()) {
                                this.createAndUpdateQueueData(brokerName, configEntry.getValue());
                            }
                        }
                    }
                }

                BrokerLiveInfo brokerLiveInfo = this.brokerLiveTable.put(brokerAddress, new BrokerLiveInfo(System.currentTimeMillis(), topicConfigSerializeWrapper.getDataVersion(),
                        channel, haServer));
                if(brokerLiveInfo == null){
                    log.info("new broker registered: {}, HAServer: {}", brokerAddress, haServer);
                }

                if(filterServerList != null){
                    if(filterServerList.isEmpty()){
                        this.filterServerTable.remove(brokerAddress);
                    }else{
                        this.filterServerTable.put(brokerAddress, filterServerList);
                    }
                }

                //如果是从节点
                if(brokerId != MixAll.MASTER_ID){
                    String masterAddress = brokerData.getBrokerAddrTable().get(MixAll.MASTER_ID);
                    if(masterAddress != null){
                        BrokerLiveInfo liveInfo = this.brokerLiveTable.get(masterAddress);
                        if(liveInfo != null){
                            result.setHaServer(haServer);
                            result.setMasterAddr(masterAddress);
                        }
                    }
                }

            } finally {
                this.lock.writeLock().unlock();
            }
        } catch (Exception e) {
            log.error("register broker exception", e);
        }

        return result;
    }

    public boolean isBrokerTopicConfigChanged(String brokerAddress, DataVersion dataVersion){
        DataVersion old = queryTopicConfig(brokerAddress);
        return old == null || !old.equals(dataVersion);
    }

    public DataVersion queryTopicConfig(final String brokerAddress){
        BrokerLiveInfo brokerLiveInfo = this.brokerLiveTable.get(brokerAddress);
        if(brokerLiveInfo != null){
            return brokerLiveInfo.getDataVersion();
        }
        return null;
    }

    public void createAndUpdateQueueData(String brokerName, TopicConfig topicConfig){
        QueueData queueData = new QueueData();
        queueData.setBrokerName(brokerName);
        queueData.setWriteQueueNums(topicConfig.getWriteQueueNums());
        queueData.setReadQueueNums(topicConfig.getReadQueueNums());
        queueData.setPerm(topicConfig.getPerm());
        queueData.setTopicSynFlag(topicConfig.getTopicSysFlag());

        List<QueueData> queueDataList = this.topicQueueTable.get(topicConfig.getTopicName());
        if (queueDataList == null) {
            queueDataList = new LinkedList<>();
            queueDataList.add(queueData);
            this.topicQueueTable.put(topicConfig.getTopicName(), queueDataList);
        }else{
            boolean addNewOne = true;
            Iterator<QueueData> iterator = queueDataList.iterator();
            while (iterator.hasNext()) {
                QueueData next = iterator.next();
                if(queueData.getBrokerName().equals(next.getBrokerName())){
                    if(next.equals(queueData)){
                        addNewOne = false;
                    }else{
                        log.warn("topic changed , {}, OLD: {}, NEW: {}", topicConfig.getTopicName(), next, queueData);
                        iterator.remove();
                    }
                }
            }
            if(addNewOne){
                queueDataList.add(queueData);
            }
        }

    }

    public void updateBrokerInfoUpdateTimestamp(String brokerAddress) {
        BrokerLiveInfo brokerLiveInfo = this.brokerLiveTable.get(brokerAddress);
        if(brokerLiveInfo != null){
            brokerLiveInfo.setLastUpdateTimestamp(System.currentTimeMillis());
        }
    }
}
