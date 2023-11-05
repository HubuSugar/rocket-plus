package edu.hubu.common.protocol.heartbeat;

/**
 * @author: sugar
 * @date: 2023/11/1
 * @description:
 */
public enum ConsumeType {
    CONSUME_ACTIVELY("PULL"),
    CONSUME_PASSIVELY("PUSH");

    private String type;

    ConsumeType(String type) {
        this.type = type;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }
}
