package edu.hubu.remoting.netty.exception;

/**
 * @author: sugar
 * @date: 2023/5/27
 * @description:
 */
public class RemotingConnectException extends RemotingException {

    public RemotingConnectException(String message) {
        this(message, null);
    }

    public RemotingConnectException(String address, Throwable cause) {
        super("connect to " + address + " failed", cause);
    }
}
