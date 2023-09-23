package edu.hubu.common.exception.broker;

/**
 * @author: sugar
 * @date: 2023/7/2
 * @description:
 */
public class MQBrokerException extends Exception{
    private final int responseCode;
    private final String errorMessage;

    public MQBrokerException(int responseCode, String errorMessage) {
        super(responseCode + ":" + errorMessage);
        this.responseCode = responseCode;
        this.errorMessage = errorMessage;
    }

    public int getResponseCode() {
        return responseCode;
    }

    public String getErrorMessage() {
        return errorMessage;
    }
}
