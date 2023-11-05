package edu.hubu.common.utils;

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

    public static final String RETRY_GROUP_TOPIC_PREFIX = "%RETRY_TOPIC%";


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
}
