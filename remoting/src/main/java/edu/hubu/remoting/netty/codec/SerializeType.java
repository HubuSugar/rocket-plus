package edu.hubu.remoting.netty.codec;

/**
 * @author: sugar
 * @date: 2023/5/22
 * @description:
 */
public enum SerializeType {
    ROCKETMQ((byte)0),
    JSON((byte)1);

    private byte code;

    public byte getCode() {
        return code;
    }

    public void setCode(byte code) {
        this.code = code;
    }

    SerializeType(byte code) {
        this.code = code;
    }

    public static SerializeType valueOf(int code){
        for (SerializeType value : values()) {
            if(value.getCode() == code){
                return value;
            }
        }
        return null;
    }
}
