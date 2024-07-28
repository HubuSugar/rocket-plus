package edu.hubu.remoting.netty.protocol;

/**
 * @author: sugar
 * @date: 2024/7/22
 * @description:
 */
public enum LanguageCode {
    JAVA((byte)0)
    ;

    private byte code;

    LanguageCode(byte code) {
        this.code = code;
    }

    public byte getCode() {
        return code;
    }

    public void setCode(byte code) {
        this.code = code;
    }
}
