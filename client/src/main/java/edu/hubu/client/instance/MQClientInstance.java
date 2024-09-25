package edu.hubu.client.instance;

import edu.hubu.client.exception.MQClientException;
import edu.hubu.client.impl.FindBrokerResult;
import edu.hubu.client.impl.MQAdminImpl;
import edu.hubu.client.impl.TopicPublishInfo;
import edu.hubu.client.impl.consumer.MQConsumerInner;
import edu.hubu.client.impl.consumer.PullMessageService;
import edu.hubu.client.impl.producer.DefaultMQProducerImpl;
import edu.hubu.client.impl.rebalance.RebalanceService;
import edu.hubu.client.producer.ClientRemotingProcessor;
import edu.hubu.client.producer.DefaultMQProducer;
import edu.hubu.client.producer.MQProducerInner;
import edu.hubu.common.PermName;
import edu.hubu.common.ServiceState;
import edu.hubu.common.protocol.heartbeat.ConsumerData;
import edu.hubu.common.protocol.heartbeat.HeartbeatData;
import edu.hubu.common.protocol.heartbeat.ProducerData;
import edu.hubu.common.protocol.heartbeat.SubscriptionData;
import edu.hubu.remoting.netty.exception.RemotingException;
import edu.hubu.common.message.MessageQueue;
import edu.hubu.common.protocol.route.BrokerData;
import edu.hubu.common.protocol.route.QueueData;
import edu.hubu.common.protocol.route.TopicRouteData;
import edu.hubu.common.utils.MixAll;
import edu.hubu.remoting.netty.NettyClientConfig;
import edu.hubu.remoting.netty.handler.RpcHook;
import lombok.extern.slf4j.Slf4j;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
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
    private final MQAdminImpl mqAdminImpl;
    private final NettyClientConfig nettyClientConfig;
    private final Lock heartBeat = new ReentrantLock();

    //<group, producer>
    private final ConcurrentMap<String, MQProducerInner> producerTable = new ConcurrentHashMap<>();
    //<group, Consumer>
    private final ConcurrentMap<String, MQConsumerInner> consumerTable = new ConcurrentHashMap<>();

    //<topic, TopicRouteData>
    private final ConcurrentMap<String, TopicRouteData> topicRouteTable = new ConcurrentHashMap<>();
    //<brokerName, brokerId, brokerAddress>
    private final ConcurrentMap<String, HashMap<Long, String>> brokerAddrTable = new ConcurrentHashMap<>();
    //<BrokerName, brokerAddr, version>
    private final ConcurrentMap<String, HashMap<String, Integer>> brokerVersionTable = new ConcurrentHashMap<>();
    private final Lock nameSrvLock = new ReentrantLock();

    //用于启动时执行定时任务
    private final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
        @Override
        public Thread newThread(Runnable r) {
            return new Thread(r,"mqClientFactoryScheduledThread");
        }
    });

    private final ClientRemotingProcessor clientRemotingProcessor;
    private final PullMessageService pullMessageService;
    private final RebalanceService rebalanceService;
    private final DefaultMQProducer defaultMQProducer;
    private ServiceState serviceState = ServiceState.CREATE_JUST;

    private final AtomicLong sendHeartbeatTimesTotal = new AtomicLong(0);

    public MQClientInstance(final ClientConfig clientConfig, int instanceIndex, String clientId, RpcHook rpcHook) {
        this.clientConfig = clientConfig;
        this.clientId = clientId;
        this.nettyClientConfig = new NettyClientConfig();
        this.clientRemotingProcessor = new ClientRemotingProcessor(this);
        this.mqClientAPI = new MQClientAPIImpl(this.nettyClientConfig, clientRemotingProcessor, rpcHook, clientConfig);

        this.mqAdminImpl = new MQAdminImpl(this);

        //更新nameSrv地址
        if(clientConfig.getNameServer() != null){
            this.mqClientAPI.updateNameSrvAddressList(clientConfig.getNameServer());
            log.info("user specified name server address: {}", clientConfig.getNameServer());
        }

        this.pullMessageService = new PullMessageService(this);
        this.rebalanceService = new RebalanceService(this);
        this.defaultMQProducer = new DefaultMQProducer(MixAll.CLIENT_INNER_PRODUCER_GROUP);
        this.defaultMQProducer.resetClientConfig(clientConfig);
    }

    /**
     * 主要是启动netty客户端
     */
    public void start() throws MQClientException {
        synchronized (this){
            switch (serviceState){
                case CREATE_JUST:
                    this.serviceState = ServiceState.START_FAILED;
                    if(this.clientConfig.getNameServer() == null){
                        this.mqClientAPI.fetchNameSrvAddress();
                    }
                    //启动netty客户端
                    this.mqClientAPI.start();
                    //start variables schedule tasks
                    this.startScheduleTasks();
                    //启动消息拉取线程，push方式
                    this.pullMessageService.start();
                    //启动rebalance线程
                    this.rebalanceService.start();
                    //
                    this.defaultMQProducer.getDefaultMQProducerImpl().start(false);
                    log.info("the client factory [{}] start ok", this.clientId);
                    this.serviceState = ServiceState.RUNNING;
                    break;
                case START_FAILED:
                    throw new MQClientException("The factory object ["+ this.clientId +"] has been created before, and failed", null);
                default:
                    break;
            }
        }
    }

    private void startScheduleTasks(){
        //1、定时获取nameSrvAddress
        if (null == this.clientConfig.getNameServer()) {
            this.scheduledExecutorService.scheduleAtFixedRate(() -> {
                try {
                    this.mqClientAPI.fetchNameSrvAddress();
                } catch (Exception e) {
                    log.error("fetch nameserver address exception", e);
                }
            }, 1000 * 10, 1000 * 60 * 2, TimeUnit.MILLISECONDS);
        }

        //2、定时获取topicRouteInfo
        this.scheduledExecutorService.scheduleAtFixedRate(() -> {
            try{
                MQClientInstance.this.updateTopicInfoFromNameServer();
            }catch (Exception e){
                log.error("schedule update topic route info from nameSrv exception", e);
            }
        }, 10, this.clientConfig.getPollNameSrvInterval(), TimeUnit.MILLISECONDS);

        //3、定期向nameserver发送心跳及清理离线的broker
        this.scheduledExecutorService.scheduleAtFixedRate(() -> {
            try{
                //todo cleanOffline broker
                this.sendHeartbeatToAllBrokerWithLock();
            }catch (Exception e){
                log.error("send heartbeat to all broker exception", e);
            }
        }, 10, this.clientConfig.getHeartbeatBrokerInterval(), TimeUnit.MILLISECONDS);

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
        Set<String> topicList = new HashSet<>();
        //consumer
        Iterator<Map.Entry<String, MQConsumerInner>> iterator = this.consumerTable.entrySet().iterator();
        while (iterator.hasNext()){
            MQConsumerInner mqConsumerInner = iterator.next().getValue();
            if(mqConsumerInner != null){
                Set<SubscriptionData> subscriptions = mqConsumerInner.subscriptions();
                if(subscriptions != null){
                    for (SubscriptionData subscription : subscriptions) {
                        topicList.add(subscription.getTopic());
                    }
                }
            }
        }

        //producer
        for (Map.Entry<String, MQProducerInner> entry : this.producerTable.entrySet()) {
            MQProducerInner producerInner = entry.getValue();
            if(producerInner != null){
                Set<String> ptl = producerInner.getPublishTopicList();
                topicList.addAll(ptl);
            }
        }

        log.info("定时更新Topic信息：size: {}, topics: {}", topicList.size(), topicList);
        for (String s : topicList) {
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
                                int queueNums = Math.min(defaultMQProducer.getDefaultQueueNums(), queueData.getReadQueueNums());
                                queueData.setReadQueueNums(queueNums);
                                queueData.setWriteQueueNums(queueNums);
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
                        }else{
                            log.info("The topic[{}] route info changed, old: {}, new: {}", topic, old, topicRouteData);
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

                            //update sub info
                            {
                                Set<MessageQueue> topicSubscribeInfo = topicRouteData2TopicSubscribeInfo(topic, topicRouteData);
                                for (Map.Entry<String, MQConsumerInner> consumerInnerEntry : this.consumerTable.entrySet()) {
                                    MQConsumerInner consumerInner = consumerInnerEntry.getValue();
                                    if (consumerInner != null) {
                                        consumerInner.updateTopicSubscribeInfo(topic, topicSubscribeInfo);
                                    }
                                }

                            }

                            log.info("update topic route table, topic: {}, topicRouteData:{}", topic, topicRouteData);
                            this.topicRouteTable.put(topic, cloneData);
                            return true;
                        }

                    }else{
                        log.warn("get topic route from name server return null: {}", topic);
                    }
                    //当首次发送topic不存在时，会先将异常捕获，然后通过默认的topic查找TopicRouteData
                }catch (MQClientException e){
                    if(!topic.startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)){
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

    public static Set<MessageQueue> topicRouteData2TopicSubscribeInfo(String topic, TopicRouteData routeData){
        Set<MessageQueue> mqList = new HashSet<>();
        List<QueueData> queueDatas = routeData.getQueueData();
        for (QueueData queueData : queueDatas) {

            if(PermName.isReadable(queueData.getPerm())){
                for (int i = 0; i < queueData.getReadQueueNums(); i++) {
                    MessageQueue messageQueue = new MessageQueue(topic, queueData.getBrokerName(), i);
                    mqList.add(messageQueue);
                }
            }

        }
        return mqList;
    }

    private boolean isNeedUpdateTopicRouteInfo(String topic){
        boolean result = false;
        //producer
        {
            Iterator<Map.Entry<String, MQProducerInner>> iterator = this.producerTable.entrySet().iterator();
            while (iterator.hasNext() && !result){
                Map.Entry<String, MQProducerInner> entry = iterator.next();
                MQProducerInner producerInner = entry.getValue();
                if(producerInner != null){
                    result = producerInner.isNeedUpdateTopicRoute(topic);
                }
            }
        }

        //consumer
        {
            Iterator<Map.Entry<String, MQConsumerInner>> iterator = this.consumerTable.entrySet().iterator();
            while (iterator.hasNext() && !result){
                Map.Entry<String, MQConsumerInner> entry = iterator.next();
                MQConsumerInner consumerInner = entry.getValue();
                if(consumerInner != null){
                    result = consumerInner.isNeedUpdateTopicRoute(topic);
                }
            }
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

        return !oldData.equals(newData);
    }

    public void doRebalance() {
        for (Map.Entry<String, MQConsumerInner> entry : this.consumerTable.entrySet()) {
            MQConsumerInner consumerInner = entry.getValue();
            if(consumerInner != null){
                try{
                    consumerInner.doRebalance();
                }catch (Exception e){
                    log.error("doRebalance exception", e);
                }
            }
        }
    }

    /**
     * producer注册
     * @param group 生产者组
     * @param producer 生产者具体实现对象
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

    public boolean registerConsumer(final String group, final MQConsumerInner consumer){
        if(group == null || consumer == null){
            return false;
        }
        MQConsumerInner prev = this.consumerTable.putIfAbsent(group, consumer);
        if(prev != null){
            log.warn(" the consumer group {} already exists", group);
            return false;
        }
        return true;
    }


    public List<String> findConsumerIdList(String topic, String consumerGroup) {
        String brokerAddr = findBrokerAddrByTopic(topic);
        if(brokerAddr == null){
            this.updateTopicInfoFromNameServer(topic);
            brokerAddr = findBrokerAddrByTopic(topic);
        }

        if(brokerAddr != null){
            try{
                return this.mqClientAPI.getConsumerIdListByGroup(brokerAddr, consumerGroup, 3000);
            }catch (Exception e){
                log.warn("getConsumerIdListByGroup exception, " + brokerAddr + " " + consumerGroup, e);
            }
        }
        return null;
    }

    public String findBrokerAddrByTopic(final String topic){
        TopicRouteData topicRouteData = this.topicRouteTable.get(topic);
        if(topicRouteData != null){
            List<BrokerData> brokers = topicRouteData.getBrokerData();
            if(!brokers.isEmpty()){
                int index = new Random().nextInt(brokers.size());
                BrokerData brokerData = brokers.get(index % brokers.size());
                return brokerData.selectBrokerAddr();
            }
        }
        return null;
    }

    public void sendHeartbeatToAllBrokerWithLock() {
        if(this.heartBeat.tryLock()){
            try{
                this.sendHeartbeatToAllBroker();
                this.updateFilterClassSource();
            }catch (Exception e){
                log.error("send heart beat exception", e);
            }finally {
                heartBeat.unlock();
            }
        }else{
            log.warn("lock heart bear failed");
        }
    }

    private void updateFilterClassSource() {

    }

    private void sendHeartbeatToAllBroker() {
       final HeartbeatData heartbeatData = this.prepareHeartbeatData();
       final boolean producerDataEmpty = heartbeatData.getProduceData().isEmpty();
       final boolean consumerDataEmpty = heartbeatData.getConsumeData().isEmpty();
       if(producerDataEmpty && consumerDataEmpty){
           log.info("send heart beat but no consumer and no producer");
           return;
       }

       if(!this.brokerAddrTable.isEmpty()){
           long times = this.sendHeartbeatTimesTotal.getAndIncrement();
           Iterator<Map.Entry<String, HashMap<Long, String>>> it = this.brokerAddrTable.entrySet().iterator();
            while (it.hasNext()){
                Map.Entry<String, HashMap<Long, String>> entry = it.next();
                String brokerName = entry.getKey();
                HashMap<Long, String> oneTable = entry.getValue();
                if(oneTable != null){
                    for (Map.Entry<Long, String> brokerEntry : oneTable.entrySet()) {
                        Long brokerId = brokerEntry.getKey();
                        String brokerAddr = brokerEntry.getValue();
                        if (brokerAddr != null) {
                            if (consumerDataEmpty) {
                                if (brokerId != MixAll.MASTER_ID) {
                                    continue;
                                }
                            }  //fix bug consumerGroup can not find

                            try {
                                int version = this.mqClientAPI.sendHeartbeat(brokerAddr, heartbeatData, 3000);
                                if (!this.brokerVersionTable.containsKey(brokerName)) {
                                    this.brokerVersionTable.put(brokerName, new HashMap<>(4));
                                }

                                this.brokerVersionTable.get(brokerName).put(brokerAddr, version);

                                if (times % 20 == 0) {
                                    log.info("send heart beat to broker [{}, {}, {}]", brokerName, brokerId, brokerAddr);
                                    log.info(heartbeatData.toString());
                                }
                            } catch (Exception e) {
                                if (this.isBrokerInNameServer(brokerAddr)) {
                                    log.info("send heart beat to broker[{},{},{}] failed", brokerName, brokerId, brokerAddr, e);
                                } else {
                                    log.info("send heart beat to broker[{},{},{}] exception", brokerName, brokerId, brokerAddr, e);
                                }
                            }
                        }
                    }
                }
            }

       }
    }

    private boolean isBrokerInNameServer(final String brokerAddr) {
        Iterator<Map.Entry<String, TopicRouteData>> iterator = this.topicRouteTable.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, TopicRouteData> next = iterator.next();
            List<BrokerData> brokerDatas = next.getValue().getBrokerData();
            for (BrokerData brokerData : brokerDatas) {
                boolean contain = brokerData.getBrokerAddrTable().containsValue(brokerAddr);
                if(contain){
                    return true;
                }
            }
        }
        return false;
    }

    private HeartbeatData prepareHeartbeatData() {
        final HeartbeatData heartbeatData = new HeartbeatData();
        //clientId
        heartbeatData.setClientId(this.clientId);

        //producerData
        for (Map.Entry<String, MQProducerInner> entry : this.producerTable.entrySet()) {
            MQProducerInner impl = entry.getValue();
            if(impl != null){
                ProducerData producerData = new ProducerData();
                producerData.setGroupName(entry.getKey());

                heartbeatData.getProduceData().add(producerData);
            }
        }

        //consumerData
        for (Map.Entry<String, MQConsumerInner> entry : this.consumerTable.entrySet()) {
            MQConsumerInner impl = entry.getValue();
            if(impl != null){
                ConsumerData consumerData = new ConsumerData();
                consumerData.setGroupName(impl.groupName());
                consumerData.setConsumerType(impl.consumeType());
                consumerData.setMessageModel(impl.messageModel());
                consumerData.setConsumeFromWhere(impl.consumeFromWhere());
                consumerData.setSubscriptionData(impl.subscriptions());
                consumerData.setUnitMode(impl.isUnitMode());

                heartbeatData.getConsumeData().add(consumerData);
            }
        }

        return heartbeatData;
    }

    public FindBrokerResult findBrokerAddressInSubscribe(
            final String brokerName,
            final long brokerId,
            final boolean onlyThisBroker) {
        String brokerAddr = null;
        boolean slave = false;
        boolean found = false;

        HashMap<Long, String> map = this.brokerAddrTable.get(brokerName);
        if(map != null && !map.isEmpty()){
            brokerAddr = map.get(brokerId);
            slave = brokerId != MixAll.MASTER_ID;
            found = brokerAddr != null;

            if(!found && !onlyThisBroker){
                Map.Entry<Long, String> next = map.entrySet().iterator().next();
                brokerAddr = next.getValue();
                slave = next.getKey() != MixAll.MASTER_ID;
                found = true;
            }
        }

        if(found){
            return new FindBrokerResult(brokerAddr, slave, findBrokerVersion(brokerName, brokerAddr));
        }

        return null;
    }

    public int findBrokerVersion(String brokerName, String brokerAddr) {
        if (brokerVersionTable.containsKey(brokerName)) {
            if (this.brokerVersionTable.get(brokerName).containsKey(brokerAddr)) {
                return this.brokerVersionTable.get(brokerName).get(brokerAddr);
            }
        }
        //to do need to fresh the version
        return 0;
    }

    public MQClientAPIImpl getMqClientAPI() {
        return mqClientAPI;
    }

    public String getClientId() {
        return clientId;
    }

    public MQAdminImpl getMqAdminImpl() {
        return mqAdminImpl;
    }

    public ConcurrentMap<String, TopicRouteData> getTopicRouteTable() {
        return topicRouteTable;
    }
}
