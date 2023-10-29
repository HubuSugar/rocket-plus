package edu.hubu.common.protocol.heartbeat;

/**
 * @author: sugar
 * @date: 2023/10/28
 * @description:
 */
public enum MessageModel {

    BROADCASTING("BROADCASTING"),
    CLUSTERING("CLUSTERING"),
    ;

    private String mode;

    MessageModel(String mode) {
        this.mode = mode;
    }

    public String getMode() {
        return mode;
    }

    public void setMode(String mode) {
        this.mode = mode;
    }
}
