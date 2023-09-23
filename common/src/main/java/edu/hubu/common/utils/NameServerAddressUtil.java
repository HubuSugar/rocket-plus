package edu.hubu.common.utils;

import java.util.regex.Pattern;

/**
 * @author: sugar
 * @date: 2023/6/4
 * @description:
 */
public class NameServerAddressUtil {
    public static final String ENDPOINT_PREFIX = "http://";
    public static final Pattern NAMESRV_ENDPOINT_PATTERN = Pattern.compile("^" + ENDPOINT_PREFIX + ".*");

    public static String getNameServerAddress(){
        return System.getProperty(MixAll.NAME_SERVER_ADDRESS_PROPERTY, System.getenv(MixAll.NAME_SERVER_ADDRESS_ENV));
    }
}
