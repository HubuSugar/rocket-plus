package edu.hubu.client.exception;

/**
 * @author: sugar
 * @date: 2023/6/3
 * @description:
 */
public class MQClientException extends Exception{

    private int responseCode;
    private String errorMessage;

    public MQClientException(String errorMessage, Throwable cause) {
        super(errorMessage, cause);
        this.responseCode = -1;
        this.errorMessage = errorMessage;
    }

    public MQClientException(int responseCode, String errorMessage) {
        super(errorMessage);
        this.responseCode = -1;
        this.errorMessage = errorMessage;
    }

    public MQClientException(int responseCode, String errorMessage, Throwable cause) {
        super(errorMessage, cause);
        this.responseCode = responseCode;
        this.errorMessage = errorMessage;
    }

    public int getResponseCode() {
        return responseCode;
    }

    public MQClientException setResponseCode(int responseCode) {
        this.responseCode = responseCode;
        return this;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public MQClientException setErrorMessage(String errorMessage) {
        this.errorMessage = errorMessage;
        return this;
    }
}
