package edu.hubu.common.topic;

import java.util.HashSet;
import java.util.Set;

/**
 * @author: sugar
 * @date: 2023/6/26
 * @description:
 */
public class TopicValidator {
    private static final Set<String> SYSTEM_TOPICS = new HashSet<>();
    public static final String AUTO_CREATE_TOPIC_KEY_TOPIC = "TBW102";
    public static final String RMQ_SYS_SCHEDULE_TOPIC = "SCHEDULE_TOPIC_XXXX";

    public static void addSystemTopic(String topicName){
        SYSTEM_TOPICS.add(topicName);
    }

    public static boolean isSystemTopic(String topicName){
        return SYSTEM_TOPICS.contains(topicName);
    }
}
