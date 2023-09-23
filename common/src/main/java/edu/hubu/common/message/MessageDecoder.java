package edu.hubu.common.message;

import edu.hubu.common.utils.UtilAll;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

/**
 * @author: sugar
 * @date: 2023/7/15
 * @description:
 */
public class MessageDecoder {

    public static final char NAME_VALUE_SEPARATOR = 1;
    public static final char PROPERTY_SEPARATOR = 2;

    public static String properties2String(Map<String, String> properties){
        StringBuilder sb = new StringBuilder();
        if(properties == null) return sb.toString();
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();
            if(value == null){
                continue;
            }
            sb.append(key).append(NAME_VALUE_SEPARATOR).append(value).append(PROPERTY_SEPARATOR);
        }
        return sb.toString();
    }

    public static Map<String, String> string2Properties(String properties){
        Map<String, String> map = new HashMap<>();
        if(properties == null || properties.length() == 0){
            return map;
        }
        String[] items = properties.split(String.valueOf(PROPERTY_SEPARATOR));
        for (String item : items) {
            String[] i = item.split(String.valueOf(NAME_VALUE_SEPARATOR));
            if(i.length == 2){
                map.put(i[0], i[1]);
            }
        }
        return map;
    }

    /**
     * 生成消息id
     * @param input 消息id的内存变量
     * @param byteBuffer 消息存储机器的地址byteBuffer
     * @param wroteOffset 消息写入commitlog的偏移量
     * @return
     */
    public static String createdMsgId(ByteBuffer input, ByteBuffer byteBuffer, long wroteOffset) {
        input.flip();
        int msgIdLength = byteBuffer.limit() == 8 ? 16 : 28;
        input.limit(msgIdLength);

        input.put(byteBuffer);
        input.putLong(wroteOffset);
        return UtilAll.byte2String(input.array());
    }
}
