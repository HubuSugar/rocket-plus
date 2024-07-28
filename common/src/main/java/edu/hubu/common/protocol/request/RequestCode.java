package edu.hubu.common.protocol.request;

/**
 * @author: sugar
 * @date: 2023/5/31
 * @description:
 */
public class RequestCode {

    public static final int HEART_BEAT = 34;

    public static final int GET_MAX_OFFSET = 30;
    public static final int GET_CONSUMER_LIST_BY_GROUP = 38;

    public static final int LOCK_BATCH_MQ = 41;

    public static final int SEND_MESSAGE = 100;
    public static final int SEND_MESSAGE_BATCH = 111;

    public static final int GET_ROUTE_INFO_BY_TOPIC = 105;
    public static final int REGISTER_BROKER = 300;
    public static final int QUERY_DATA_VERSION = 301;

    public static final int PULL_MESSAGE = 410;
}
