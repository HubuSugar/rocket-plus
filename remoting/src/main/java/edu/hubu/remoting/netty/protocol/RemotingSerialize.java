package edu.hubu.remoting.netty.protocol;

import com.alibaba.fastjson.JSON;

import java.nio.charset.StandardCharsets;

/**
 * @author: sugar
 * @date: 2023/5/22
 * @description:
 */
public abstract class RemotingSerialize {

    public static byte[] encode(final Object obj){
        String json = toJson(obj, false);
        return json.getBytes(StandardCharsets.UTF_8);
    }

    public static String toJson(final Object obj, boolean prettyFormat){
        return JSON.toJSONString(obj, prettyFormat);
    }

    public static <T> T decode(byte[] in, Class<T> clazz){
        return JSON.parseObject(new String(in, StandardCharsets.UTF_8), clazz);
    }

    public byte[] encode() {
        String json = this.toJson();
        if (json != null) {
            return json.getBytes(StandardCharsets.UTF_8);
        }
        return null;
    }

    public String toJson(){
       return toJson(false);
    }

    public String toJson(boolean prettyFormat){
        return JSON.toJSONString(this, prettyFormat);
    }
}
