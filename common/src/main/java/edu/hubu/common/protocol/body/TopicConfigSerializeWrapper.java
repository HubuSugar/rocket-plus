package edu.hubu.common.protocol.body;

import edu.hubu.common.DataVersion;
import edu.hubu.common.TopicConfig;
import edu.hubu.remoting.netty.protocol.RemotingSerialize;

import java.util.concurrent.ConcurrentHashMap;

/**
 * @author: sugar
 * @date: 2023/6/11
 * @description: 包装topicConfig用于乐观锁
 */
public class TopicConfigSerializeWrapper extends RemotingSerialize {
    private ConcurrentHashMap<String, TopicConfig> topicConfigTable = new ConcurrentHashMap<>();
    private DataVersion dataVersion = new DataVersion();

    public ConcurrentHashMap<String, TopicConfig> getTopicConfigTable() {
        return topicConfigTable;
    }

    public DataVersion getDataVersion() {
        return dataVersion;
    }

    public void setTopicConfigTable(ConcurrentHashMap<String, TopicConfig> topicConfigTable) {
        this.topicConfigTable = topicConfigTable;
    }

    public void setDataVersion(DataVersion dataVersion) {
        this.dataVersion = dataVersion;
    }
}
