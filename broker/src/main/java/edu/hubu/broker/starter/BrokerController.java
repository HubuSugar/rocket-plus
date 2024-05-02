package edu.hubu.broker.starter;

import edu.hubu.broker.client.ConsumerManager;
import edu.hubu.broker.client.rebalance.RebalanceLockManager;
import edu.hubu.broker.filter.ConsumerFilterManager;
import edu.hubu.broker.filtersrv.FilterServerManager;
import edu.hubu.broker.longpolling.NotifyMessageArrivingListener;
import edu.hubu.broker.longpolling.PullRequestHoldService;
import edu.hubu.broker.out.BrokerOutAPI;
import edu.hubu.broker.processor.AdminBrokerProcessor;
import edu.hubu.broker.processor.ConsumerManagerProcessor;
import edu.hubu.broker.processor.PullMessageProcessor;
import edu.hubu.broker.processor.SendMessageProcessor;
import edu.hubu.broker.subscription.SubscriptionGroupManager;
import edu.hubu.broker.topic.TopicConfigManager;
import edu.hubu.common.BrokerConfig;
import edu.hubu.common.PermName;
import edu.hubu.common.TopicConfig;
import edu.hubu.common.protocol.body.TopicConfigSerializeWrapper;
import edu.hubu.common.protocol.request.RequestCode;
import edu.hubu.common.protocol.result.RegisterBrokerResult;
import edu.hubu.remoting.netty.NettyClientConfig;
import edu.hubu.remoting.netty.NettyRemotingServer;
import edu.hubu.remoting.netty.NettyServerConfig;
import edu.hubu.store.DefaultMessageStore;
import edu.hubu.store.MessageStore;
import edu.hubu.store.config.MessageStoreConfig;
import edu.hubu.store.listen.MessageArrivingListener;
import lombok.extern.slf4j.Slf4j;

import java.io.FileNotFoundException;
import java.util.List;
import java.util.concurrent.*;

/**
 * @author: sugar
 * @date: 2023/5/24
 * @description:
 */
@Slf4j
public class BrokerController {

    private final BrokerConfig brokerConfig;
    private final NettyServerConfig nettyServerConfig;
    private final NettyClientConfig nettyClientConfig;
    private final MessageStoreConfig messageStoreConfig;
    private NettyRemotingServer nettyRemotingServer;

    private final BlockingQueue<Runnable> sendMessageThreadQueue;
    private final BlockingQueue<Runnable> pullMessageThreadQueue;
    private ExecutorService sendMessageThreadExecutor;
    private ExecutorService pullMessageExecutor;
    private ExecutorService consumerManagerExecutor;
    private ExecutorService adminBrokerExecutor;

    private final RebalanceLockManager rebalanceLockManager = new RebalanceLockManager();
    private final BrokerOutAPI brokerOutAPI;

    private TopicConfigManager topicConfigManager;
    private final FilterServerManager filterServerManager;

    private MessageStore messageStore;
    private final PullRequestHoldService pullHoldService;
    private final MessageArrivingListener messageArrivingListener;
    private final SubscriptionGroupManager subscriptionGroupManager;
    private final PullMessageProcessor pullMessageProcessor;

    private final ConsumerManager consumerManager;
    private final ConsumerFilterManager consumerFilterManager;


    private final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
        @Override
        public Thread newThread(Runnable r) {
            return new Thread(r, "brokerControllerScheduleThread");
        }
    });

    public BrokerController(final BrokerConfig brokerConfig,
                            final NettyServerConfig nettyServerConfig,
                            final NettyClientConfig nettyClientConfig,
                            final MessageStoreConfig messageStoreConfig) {
        this.nettyServerConfig = nettyServerConfig;
        this.nettyClientConfig = nettyClientConfig;
        this.brokerConfig = brokerConfig;
        this.messageStoreConfig = messageStoreConfig;

        this.subscriptionGroupManager = new SubscriptionGroupManager(this);
        this.brokerOutAPI = new BrokerOutAPI(nettyClientConfig);


        this.sendMessageThreadQueue = new LinkedBlockingQueue<>(brokerConfig.getSendMessageQueueCapacity());
        this.pullMessageThreadQueue = new LinkedBlockingQueue<>(brokerConfig.getPullThreadQueueCapacity());


        this.topicConfigManager = new TopicConfigManager(this);
        this.filterServerManager = new FilterServerManager(this);

        this.pullHoldService = new PullRequestHoldService(this);
        this.messageArrivingListener = new NotifyMessageArrivingListener(this.pullHoldService);
        this.consumerManager = new ConsumerManager();
        this.consumerFilterManager = new ConsumerFilterManager(this);

        this.pullMessageProcessor = new PullMessageProcessor(this);
    }

    public void initialize(){
        //加载持久化的topicConfig配置
        boolean result = this.topicConfigManager.load();

        if(result){
            //
            try {
                this.messageStore = new DefaultMessageStore(this.brokerConfig, this.messageStoreConfig, this.messageArrivingListener);
            } catch (FileNotFoundException e) {
                log.error("new message store exception", e);
            }
        }

        result = result && this.messageStore.load();

        if(result){
            this.nettyRemotingServer = new NettyRemotingServer(this.nettyServerConfig);
            this.sendMessageThreadExecutor = new ThreadPoolExecutor(this.brokerConfig.getSendMessageThreadNums(),
                    this.brokerConfig.getSendMessageThreadNums(), 1000 * 60, TimeUnit.MILLISECONDS, this.sendMessageThreadQueue);

            this.pullMessageExecutor = new ThreadPoolExecutor(
                    this.brokerConfig.getPullMessageThreadNums(),
                    this.brokerConfig.getPullMessageThreadNums(),
                    1000 * 60,
                    TimeUnit.MILLISECONDS,
                    this.pullMessageThreadQueue
            );

            this.consumerManagerExecutor = Executors.newFixedThreadPool(this.brokerConfig.getConsumerManageThreadNums(), new ThreadFactory() {
                @Override
                public Thread newThread(Runnable r) {
                    return new Thread(r, "consumerManagerThreadExecutor");
                }
            });
            this.adminBrokerExecutor = Executors.newFixedThreadPool(this.brokerConfig.getAdminBrokerThreadNums(), new ThreadFactory() {
                @Override
                public Thread newThread(Runnable r) {
                    return new Thread(r, "adminBrokerThreadExecutor");
                }
            });


            //初始化注册processor
            this.registerProcessor();

            if(this.brokerConfig.getNameSrvAddress() != null){
                this.brokerOutAPI.updateNameSrvAddressList(this.brokerConfig.getNameSrvAddress());
            }else if(this.brokerConfig.isFetchNameSrvByAddressServer()){
                //
            }
        }

    }

    /**
     * brokerAddress = broker local IP : nettyServerPort
     * @return
     */
    public String getBrokerAddr(){
        return brokerConfig.getBrokerIP1() + ":" + this.nettyServerConfig.getListenPort();
    }

    public String getHAServerAddr(){
        return brokerConfig.getBrokerIP2() + ":" + this.nettyServerConfig.getListenPort();
    }

    public void start() throws Exception {
        if(messageStore != null){
            this.messageStore.start();
        }

        if (this.nettyRemotingServer != null) {
            this.nettyRemotingServer.start();
        }
        if (this.brokerOutAPI != null) {
            this.brokerOutAPI.start();
        }

        //定时向nameSrv注册broker信息
        this.scheduledExecutorService.scheduleAtFixedRate(() -> {
            BrokerController.this.registerAllBroker(true, false, brokerConfig.isForceRegister());
        }, 1000 * 10, Math.max(1000 * 10, Math.min(brokerConfig.getRegisterNameSrvPeriod(), 1000 * 160)), TimeUnit.MILLISECONDS);

    }

    public void registerProcessor() {
        SendMessageProcessor sendMessageProcessor = new SendMessageProcessor(this);
        this.nettyRemotingServer.registerProcessor(RequestCode.SEND_MESSAGE, sendMessageProcessor, this.sendMessageThreadExecutor);

        //拉取消息pullMessageProcessor
        this.nettyRemotingServer.registerProcessor(RequestCode.PULL_MESSAGE, this.pullMessageProcessor, this.pullMessageExecutor);

        //消费者管理processor
        ConsumerManagerProcessor consumerProcessor = new ConsumerManagerProcessor(this);
        this.nettyRemotingServer.registerProcessor(RequestCode.GET_CONSUMER_LIST_BY_GROUP, consumerProcessor, this.consumerManagerExecutor);

        //默认processor
        AdminBrokerProcessor adminProcessor = new AdminBrokerProcessor(this);
        this.nettyRemotingServer.registerDefaultProcessor(adminProcessor, this.adminBrokerExecutor);

    }


    /**
     * Broker启动时向nameSrv注册
     * @param checkOrderConf
     * @param oneway
     * @param forceRegister
     */
    public synchronized void registerAllBroker(boolean checkOrderConf, boolean oneway, boolean forceRegister) {
        TopicConfigSerializeWrapper topicConfigSerializeWrapper = this.getTopicConfigManager().buildTopicConfigSerializeWrapper();
        if(!PermName.isWritable(this.getBrokerConfig().getBrokerPermission())
            ||!PermName.isReadable(this.getBrokerConfig().getBrokerPermission())){
            ConcurrentHashMap<String, TopicConfig> map = new ConcurrentHashMap<>();

            for (TopicConfig config : topicConfigSerializeWrapper.getTopicConfigTable().values()) {
                TopicConfig topicConfig = new TopicConfig(config.getTopicName(), config.getReadQueueNums(), config.getWriteQueueNums(), brokerConfig.getBrokerPermission());
                map.put(config.getTopicName(), topicConfig);
            }

            topicConfigSerializeWrapper.setTopicConfigTable(map);
        }

        if(forceRegister || needRegister(this.brokerConfig.getBrokerClusterName(),
                this.getBrokerAddr(), this.brokerConfig.getBrokerName(), this.brokerConfig.getBrokerId(), this.brokerConfig.getRegisterBrokerTimeoutMillis())){
            doRegisterBroker(checkOrderConf, oneway, topicConfigSerializeWrapper);
        }
    }


    private void doRegisterBroker(boolean checkOrderConf, boolean oneway, TopicConfigSerializeWrapper topicConfigSerializeWrapper) {
        List<RegisterBrokerResult> registerBrokerResults = this.brokerOutAPI.registerBrokerAll(
                this.brokerConfig.getBrokerClusterName(),
                this.getBrokerAddr(),
                this.brokerConfig.getBrokerName(),
                this.brokerConfig.getBrokerId(),
                this.getHAServerAddr(),
                topicConfigSerializeWrapper,
                this.filterServerManager.buildNewFilterServerList(),
                oneway,
                this.brokerConfig.getRegisterBrokerTimeoutMillis(),
                this.brokerConfig.isCompressedRegister()
        );
        if (registerBrokerResults.size() > 0) {
            RegisterBrokerResult registerBrokerResult = registerBrokerResults.get(0);
            //同步从节点
        }

    }


    private boolean needRegister(final String clusterName, final String brokerAddress, final String brokerName, final long brokerId, final int registerBrokerTimeoutMillis){
        TopicConfigSerializeWrapper topicConfigSerializeWrapper = this.getTopicConfigManager().buildTopicConfigSerializeWrapper();
        List<Boolean> changeList = brokerOutAPI.needRegister(clusterName, brokerAddress, brokerName, brokerId, topicConfigSerializeWrapper, registerBrokerTimeoutMillis);

        boolean needRegister = false;
        for (Boolean changed : changeList) {
            if(changed){
                needRegister = true;
                break;
            }
        }
        return needRegister;
    }



    public TopicConfigManager getTopicConfigManager() {
        return topicConfigManager;
    }

    public void setTopicConfigManager(TopicConfigManager topicConfigManager) {
        this.topicConfigManager = topicConfigManager;
    }

    public BrokerConfig getBrokerConfig() {
        return brokerConfig;
    }

    public NettyServerConfig getNettyServerConfig() {
        return nettyServerConfig;
    }

    public MessageStoreConfig getMessageStoreConfig() {
        return messageStoreConfig;
    }

    public MessageStore getMessageStore() {
        return messageStore;
    }

    public RebalanceLockManager getRebalanceLockManager() {
        return rebalanceLockManager;
    }

    public ConsumerManager getConsumerManager() {
        return consumerManager;
    }

    public SubscriptionGroupManager getSubscriptionGroupManager() {
        return subscriptionGroupManager;
    }

    public PullRequestHoldService getPullHoldService() {
        return pullHoldService;
    }

    public ConsumerFilterManager getConsumerFilterManager() {
        return consumerFilterManager;
    }
}
