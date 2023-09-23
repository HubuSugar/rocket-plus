package edu.hubu.common.message;

import java.util.HashMap;
import java.util.Map;

/**
 * @author: sugar
 * @date: 2023/7/15
 * @description:
 */
public class MessageAccessor {

    public static void setProperties(final Message message, final Map<String, String> properties){
        message.setProperties(properties);
    }

    public static void putProperty(final Message message, final String key, String value){
        message.putProperty(key, value);
    }
}
