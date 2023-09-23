package edu.hubu.common.message;

import java.lang.reflect.Method;

/**
 * @author: sugar
 * @date: 2023/8/19
 * @description:
 */
public class MessageClientIDSetter {

    public static String getUniqID(final Message message){
        return message.getProperty(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX);
    }
}
