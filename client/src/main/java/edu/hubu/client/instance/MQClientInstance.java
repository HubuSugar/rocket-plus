package edu.hubu.client.instance;

import edu.hubu.client.exception.MQClientException;
import edu.hubu.client.impl.TopicPublishInfo;
import edu.hubu.client.impl.producer.DefaultMQProducerImpl;
import edu.hubu.client.producer.DefaultMQProducer;
import edu.hubu.client.producer.MQProducerInner;
import edu.hubu.common.PermName;
import edu.hubu.remoting.netty.exception.RemotingException;
import edu.hubu.common.message.MessageQueue;
import edu.hubu.common.protocol.route.BrokerData;
import edu.hubu.common.protocol.route.QueueData;
import edu.hubu.common.protocol.route.TopicRouteData;
import edu.hubu.common.utils.MixAll;
import edu.hubu.remoting.netty.NettyClientConfig;
import lombok.extern.slf4j.Slf4j;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author: sugar
 * @date: 2023/5/26
 * @description:
 */
@Slf4j
public class MQClientInstance {
    private static final long LOCK_TIMEOUT = 1000 * 3;
    private final String clientId;
    private final ClientConfig clientConfig;
    private final MQClientAPIImpl mqClientAPI;
    private final NettyClientConfig nettyClientConfig;

    //<group, producer>
    private final ConcurrentMap<String, MQProducerInner> producerTable = new ConcurrentHashMap<>();

    //<topic, TopicRouteData>
    private final ConcurrentMap<String, TopicRouteData> topicRouteTable = new ConcurrentHashMap<>();
    //<brokerName, brokerId, brokerAddress>
    private final ConcurrentMap<String, HashMap<Long, String>> brokerAddrTable = new ConcurrentHashMap<>();
    private final Lock nameSrvLock = new ReentrantLock();

    //用于启动时执行定时任务
    private final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
        @Override
        public Thread newThread(Runnable r) {
            return new Thread(r,"mqClientFactoryScheduledThread");
        }
    });

    public MQClientInstance(ClientConfig clientConfig, int instanceIndex, String clientId) {
        this.clientConfig = clientConfig;
        this.clientId = clientId;
        this.nettyClientConfig = new NettyClientConfig();
        this.mqClientAPI = new MQClientAPIImpl(this.nettyClientConfig, clientConfig);

        //更新nameSrv地址
        if(clientConfig.getNameServer() != null){
            this.mqClientAPI.updateNameSrvAddressList(clientConfig.getNameServer());
        }
    }

    public void start(){
        synchronized (this){
            if(this.clientConfig.getNameServer() == null){
                this.mqClientAPI.fetchNameSrvAddress();
            }
            this.mqClientAPI.start();
            //start variables schedule tasks
            this.startScheduleTasks();
        }
    }

    private void startScheduleTasks(){
        //1、定时获取nameSrvAddress

        //2、定时获取topicRouteInfo
        this.scheduledExecutorService.scheduleAtFixedRate(() -> {
            try{
                MQClientInstance.this.updateTopicInfoFromNameServer();
            }catch (Exception e){
                log.error("schedule update topic route info from nameSrv exception", e);
            }
        }, 10, this.clientConfig.getPollNameSrvInterval(), TimeUnit.MILLISECONDS);

        //3、定时清理离线的broker

        //4、持久化消费进度

        //5、rebalanced动态调整线程池

    }


    public String findBrokerAddressInPublish(String brokerName){
        HashMap<Long, String> brokerAddress = this.brokerAddrTable.get(brokerName);
        if(brokerAddress != null && !brokerAddress.isEmpty()){
            return brokerAddress.get(MixAll.MASTER_ID);
        }
        return null;
    }

    /**
     * mqClientInstance启动后，定时任务更新topicRouteInfo
     */
    public void updateTopicInfoFromNameServer(){
        Set<String> list = new HashSet<>();
        //consumer

        //producer
        for (Map.Entry<String, MQProducerInner> entry : this.producerTable.entrySet()) {
            MQProducerInner producerInner = entry.getValue();
            if(producerInner != null){
                Set<String> ptl = producerInner.getPublishTopicList();
                list.addAll(ptl);
            }
        }

        for (String s : list) {
            this.updateTopicInfoFromNameServer(s);
        }
    }

    public void updateTopicInfoFromNameServer(final String topic){
        updateTopicInfoFromNameServer(topic, false, null);
    }

    public boolean updateTopicInfoFromNameServer(final String topic, boolean isDefault, DefaultMQProducer defaultMQProducer){
        try {
            if (nameSrvLock.tryLock(LOCK_TIMEOUT, TimeUnit.MILLISECONDS)) {
                TopicRouteData topicRouteData;
                try{
                    if(isDefault && defaultMQProducer != null){
                        //如果是默认
                        topicRouteData = this.mqClientAPI.getDefaultTopicRouteFromNameSrv(defaultMQProducer.getCreateTopicKey(), 1000 * 3);
                        if(topicRouteData != null){
                            for (QueueData queueData : topicRouteData.getQueueData()) {
                                int queueNum = Math.min(defaultMQProducer.getDefaultQueueNums(), queueData.getReadQueueNums());
                                queueData.setReadQueueNums(queueNum);
                                queueData.setWriteQueueNums(queueNum);
                            }
                        }
                    }else{
                        topicRouteData = this.mqClientAPI.getTopicRouteFromNameSrv(topic, 1000 * 3);
                    }

                    if(topicRouteData != null){
                        //判断是否需要更新
                        TopicRouteData old = topicRouteTable.get(topic);
                        boolean changed = topicRouteDataHasChanged(old, topicRouteData);
                        if(!changed){
                            changed = this.isNeedUpdateTopicRouteInfo(topic);
                        }

                        if(changed){
                            TopicRouteData cloneData = topicRouteData.cloneTopicRouteInfo();

                            //update brokerAdd
                            for (BrokerData brokerData : cloneData.getBrokerData()) {
                                this.brokerAddrTable.put(brokerData.getBrokerName(), brokerData.getBrokerAddrTable());
                            }

                            {
                                //update pub info
                                TopicPublishInfo topicPublishInfo = topicRouteData2TopicPublishInfo(topic, topicRouteData);
                                topicPublishInfo.setHaveTopicRouteInfo(true);
                                for (Map.Entry<String, MQProducerInner> entry : this.producerTable.entrySet()) {
                                    MQProducerInner producerInner = entry.getValue();
                                    if(producerInner != null){
                                        producerInner.updateTopicPublishInfo(topic, topicPublishInfo);
                                    }
                                }
                            }

                            //update sub info todo
                            log.info("update topic route table, topic: {}, topicRouteData:{}", topic, topicRouteData);
                            this.topicRouteTable.put(topic, cloneData);
                            return true;
                        }

                    }else{
                        log.warn("get topic route from name server return null: {}", topic);
                    }
                    //当首次发送topic不存在时，会先将异常捕获，然后通过默认的topic查找TopicRouteData
                }catch (MQClientException e){
                    if(!topic.startsWith("RETRY")){
                        log.warn("updateTopicRouteInfoFromNameServer mqClientException", e);
                    }
                }catch (RemotingException e){
                    log.error("updateTopicRouteInfoFromNameServer remoting exception", e);
                    throw new IllegalStateException(e);
                }finally {
                    this.nameSrvLock.unlock();
                }
            }
        } catch (InterruptedException e) {
            log.error("updateTopicRouteInfoFromNameServer exception", e);
        }
        return false;
    }

    public static TopicPublishInfo topicRouteData2TopicPublishInfo(String topic, TopicRouteData route) {
        TopicPublishInfo topicPublishInfo = new TopicPublishInfo();
        topicPublishInfo.setTopicRouteData(route);
        if (route.getOrderTopicConf() != null && route.getOrderTopicConf().length() > 0) {
            String[] brokers = route.getOrderTopicConf().split(";");
            for (String broker : brokers) {
                String[] items = broker.split(":");
                int queueNum = Integer.parseInt(items[1]);
                for (int i = 0; i < queueNum; i++) {
                    MessageQueue mq = new MessageQueue(topic, items[0], i);
                    topicPublishInfo.getMessageQueues().add(mq);
                }
            }
            topicPublishInfo.setOrderTopic(true);
        } else {
            List<QueueData> queueData = route.getQueueData();
            Collections.sort(queueData);
            for (QueueData qd : queueData) {
                if(PermName.isWritable(qd.getPerm())){
                    BrokerData brokerData = null;
                    for(BrokerData bd: route.getBrokerData()){
                        if(qd.getBrokerName().equals(bd.getBrokerName())){
                            brokerData = bd;
                            break;
                        }
                    }

                    if(brokerData == null){
                        continue;
                    }

                    if(!brokerData.getBrokerAddrTable().containsKey(MixAll.MASTER_ID)){
                        continue;
                    }

                    for (int i = 0; i < qd.getWriteQueueNums(); i++) {
                        MessageQueue messageQueue = new MessageQueue(topic, qd.getBrokerName(), i);
                        topicPublishInfo.getMessageQueues().add(messageQueue);
                    }
                }
            }
            topicPublishInfo.setOrderTopic(false);
        }


        return topicPublishInfo;
    }

    private boolean isNeedUpdateTopicRouteInfo(String topic){
        boolean result = false;
        //producer
        {
            Iterator<Map.Entry<String, MQProducerInner>> iterator = this.producerTable.entrySet().iterator();
            while (iterator.hasNext() && !result){
                Map.Entry<String, MQProducerInner> entry = iterator.next();
                MQProducerInner producerInner = entry.getValue();
                result = producerInner.isNeedUpdateTopicRoute(topic);
            }
        }

        //consumer
        {

        }
        return result;
    }

    private boolean topicRouteDataHasChanged(TopicRouteData old, TopicRouteData topicRouteData) {
        if(old == null || topicRouteData == null){
            return true;
        }
        TopicRouteData oldData = old.cloneTopicRouteInfo();
        TopicRouteData newData = topicRouteData.cloneTopicRouteInfo();
        Collections.sort(oldData.getQueueData());
        Collections.sort(oldData.getBrokerData());
        Collections.sort(newData.getQueueData());
        Collections.sort(newData.getBrokerData());

        return !old.equals(topicRouteData);
    }

    /**
     * producer注册
     * @param group
     * @param producer
     * @return
     */
    public boolean registerProducer(final String group, final DefaultMQProducerImpl producer){
        if(group == null || null == producer){
            return false;
        }
        MQProducerInner prev = this.producerTable.putIfAbsent(group, producer);
        if(prev != null){
            log.warn(" the producer group {} already exists", group);
            return false;
        }
        return true;
    }

    public MQClientAPIImpl getMqClientAPI() {
        return mqClientAPI;
    }

    public String getClientId() {
        return clientId;
    }
}
