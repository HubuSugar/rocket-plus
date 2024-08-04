package edu.hubu.broker.topic;

import edu.hubu.broker.starter.BrokerController;
import edu.hubu.common.ConfigManager;
import edu.hubu.common.DataVersion;
import edu.hubu.common.PermName;
import edu.hubu.common.TopicConfig;
import edu.hubu.common.protocol.body.TopicConfigSerializeWrapper;
import edu.hubu.common.topic.TopicValidator;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author: sugar
 * @date: 2023/6/27
 * @description:
 */
@Slf4j
public class TopicConfigManager extends ConfigManager {

    private static final long LOCK_TIMEOUT_MILLIS = 3 * 1000;
    private final ConcurrentHashMap<String, TopicConfig> topicConfigTable = new ConcurrentHashMap<>(1024);
    private final DataVersion dataVersion = new DataVersion();
    private transient BrokerController brokerController;

    private final Lock topicConfigLock = new ReentrantLock();


    public TopicConfigManager(BrokerController brokerController) {
        this.brokerController = brokerController;
        //initSystemTopic
        this.initSystemTopicConfig();
    }


    public boolean load() {


        return true;
    }


    public TopicConfigSerializeWrapper buildTopicConfigSerializeWrapper() {
        TopicConfigSerializeWrapper topicConfigSerializeWrapper = new TopicConfigSerializeWrapper();
        topicConfigSerializeWrapper.setTopicConfigTable(topicConfigTable);
        topicConfigSerializeWrapper.setDataVersion(dataVersion);
        return topicConfigSerializeWrapper;
    }

    private void initSystemTopicConfig(){
        {
            //系统DefaultTopic
            if (this.brokerController.getBrokerConfig().isAutoCreateTopicEnable()) {
                String topic = TopicValidator.AUTO_CREATE_TOPIC_KEY_TOPIC;
                TopicConfig topicConfig = new TopicConfig(topic);
                TopicValidator.addSystemTopic(topic);
                topicConfig.setReadQueueNums(this.brokerController.getBrokerConfig()
                        .getDefaultTopicQueueNums());
                topicConfig.setWriteQueueNums(this.brokerController.getBrokerConfig()
                        .getDefaultTopicQueueNums());
                int perm = PermName.PERM_INHERIT | PermName.PERM_READABLE | PermName.PERM_WRITABLE;
                topicConfig.setPerm(perm);
                this.topicConfigTable.put(topicConfig.getTopicName(), topicConfig);
            }
        }
    }

    /**
     * 判断当前topic是不是顺序topic
     * @param topic
     * @return
     */
    public boolean isOrderTopic(String topic) {
        TopicConfig config = this.topicConfigTable.get(topic);
        if(config == null){
            return false;
        }else{
            return config.isOrder();
        }
    }

    public TopicConfig selectTopicConfig(String topic) {
        return this.topicConfigTable.get(topic);
    }

    /**
     * broker processor处理消息时没有对应的topicConfig时调用
     */
    public TopicConfig createTopicInSendMessageMethod(String topic, String defaultTopic, Integer defaultTopicQueueNums, String remoteAddr, int topicSysFlag) {
        TopicConfig config = null;
        boolean createNew = false;
        try {
            if(topicConfigLock.tryLock(LOCK_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)){
                try{
                    config = this.topicConfigTable.get(topic);
                    if(config != null){
                        return config;
                    }
                    TopicConfig defaultTopicConfig = this.topicConfigTable.get(defaultTopic);
                    if(defaultTopicConfig != null){
                        if(defaultTopic.equals(TopicValidator.AUTO_CREATE_TOPIC_KEY_TOPIC)){
                            //不是自动创建topic功能，改变defaultTopic的权限，这样真正的topic就不具有inherit权限
                            if(!this.brokerController.getBrokerConfig().isAutoCreateTopicEnable()){
                                defaultTopicConfig.setPerm(PermName.PERM_WRITABLE | PermName.PERM_READABLE);
                            }
                        }

                        if(PermName.isInherited(defaultTopicConfig.getPerm())){
                            config = new TopicConfig(topic);

                            int queueNums = Math.min(defaultTopicConfig.getWriteQueueNums(), defaultTopicQueueNums);
                            if(queueNums < 0){
                                queueNums = 0;
                            }

                            config.setWriteQueueNums(queueNums);
                            config.setWriteQueueNums(queueNums);

                            int perm = defaultTopicConfig.getPerm();
                            perm &= ~PermName.PERM_INHERIT;
                            config.setPerm(perm);

                            config.setTopicSysFlag(topicSysFlag);
                            config.setTopicFilterType(defaultTopicConfig.getTopicFilterType());

                        }else{
                            log.warn("created new topic failed, because the default topic has no perm");
                        }
                    }else{
                        log.warn("created new topic failed, because the default topic not exist");
                    }

                    if(config != null){
                        log.warn("created new topic :{} by default topic config: {}",topic, defaultTopicConfig);
                        this.topicConfigTable.put(topic, config);
                        this.dataVersion.nextVersion();
                        createNew = true;

                        //持久化
                    }
                }finally {
                    topicConfigLock.unlock();
                }
            }else{
                log.warn("lock topic config table time out");
            }
        } catch (InterruptedException e) {
            log.error("lock topic config interrupted exception", e);
        }

        if(createNew){
            this.brokerController.registerAllBroker(false, true, true);
        }

        return config;
    }

    public TopicConfig createTopicInSendMessageBackMethod(String newTopic, int defaultClientQueueNums, int permission, int sysFlag) {
        TopicConfig topicConfig = this.topicConfigTable.get(newTopic);
        if(topicConfig != null){
            return topicConfig;
        }
        boolean createNew = false;
        try{
            if (this.topicConfigLock.tryLock(LOCK_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)) {
                try{
                    topicConfig = this.topicConfigTable.get(newTopic);
                    if (topicConfig != null){
                        return topicConfig;
                    }
                    topicConfig = new TopicConfig();
                    topicConfig.setReadQueueNums(defaultClientQueueNums);
                    topicConfig.setWriteQueueNums(defaultClientQueueNums);
                    topicConfig.setPerm(permission);
                    topicConfig.setTopicSysFlag(sysFlag);

                    log.info("create new topic: {}", topicConfig);
                    this.topicConfigTable.put(newTopic, topicConfig);
                    createNew = true;
                    this.dataVersion.nextVersion();
                    this.persist();

                }finally {
                    this.topicConfigLock.unlock();
                }
            }
        }catch (InterruptedException e){
            log.error("create new topic in send message back exception", e);
        }

        if(createNew){
            this.brokerController.registerAllBroker(false, true, true);
        }
        return topicConfig;
    }
}
