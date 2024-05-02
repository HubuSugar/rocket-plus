package edu.hubu.remoting.netty;

import org.omg.CORBA.PUBLIC_MEMBER;

/**
 * @author: sugar
 * @date: 2023/5/21
 * @description:
 */
public class ResponseCode {
    public static final int SUCCESS = 0;
    public static final int SYSTEM_ERROR = 1;
    public static final int SYSTEM_BUSY = 2;
    public static final int UNSUPPORTED_REQUEST_CODE = 10;
    public static final int SERVICE_NOT_AVAILABLE = 14;

    public static final int TOPIC_NOT_EXIST = 17;

    public static final int PULL_NOT_FOUND = 19;
    public static final int PULL_RETRY_IMMEDIATELY = 20;
    public static final int PULL_OFFSET_MOVED = 21;

    public static final int SUBSCRIPTION_PARSE_FAILED = 23;
    public static final int SUBSCRIPTION_NOT_EXISTS = 24;
    public static final int SUBSCRIPTION_NOT_LATEST = 25;
    public static final int SUBSCRIPTION_GROUP_NOT_EXISTS = 26;

    public static final int FLUSH_DISK_TIMEOUT = 31;
    public static final int FLUSH_SLAVE_TIMEOUT = 32;
    public static final int SLAVE_NOT_AVAILABLE= 33;

    public static final int NO_PERMISSION = 51;
    public static final int MESSAGE_ILLEGAL = 52;

}
