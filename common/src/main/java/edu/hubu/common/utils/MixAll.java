package edu.hubu.common.utils;

import java.util.concurrent.atomic.AtomicLong;

/**
 * @author: sugar
 * @date: 2023/6/4
 * @description:
 */
public class MixAll {
    public static final String NAME_SERVER_ADDRESS_PROPERTY = "rocketmq.nameserver.address";
    public static final String NAME_SERVER_ADDRESS_ENV = "NAMESERVER_ADDR";
    public static final long MASTER_ID = 0L;
    public static final String DEFAULT_TRACE_REGION = "DefaultRegion";

    public static final String RETRY_GROUP_TOPIC_PREFIX = "%RETRY%";
    public static final String CID_RMQ_SYS_PREFIX = "CIS_RMQ_SYS_";

    public static String getRetryTopic(final String groupName) {
        return RETRY_GROUP_TOPIC_PREFIX + groupName;
    }

    public static String brokerVipChannel(boolean vipChannelEnable, String brokerAddr) {
        if(vipChannelEnable){
            int lastIndex = brokerAddr.lastIndexOf(":");
            String ip = brokerAddr.substring(0, lastIndex);
            String port = brokerAddr.substring(lastIndex + 1);

            return ip + ":" + (Integer.parseInt(port) - 2);
        }else{
            return brokerAddr;
        }
    }

    public static boolean isSysConsumerGroup(final String consumerGroup) {
        return consumerGroup.startsWith(CID_RMQ_SYS_PREFIX);
    }

    public static boolean compareAndIncreaseOnly(final AtomicLong target, final long value) {
        long prev = target.get();
        while (value > prev){
            boolean updated = target.compareAndSet(prev, value);
            if(updated){
                return true;
            }
            prev = target.get();
        }
        return false;
    }
}
