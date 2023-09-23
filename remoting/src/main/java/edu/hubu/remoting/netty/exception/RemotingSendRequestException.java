package edu.hubu.remoting.netty.exception;

/**
 * @author: sugar
 * @date: 2023/5/27
 * @description:
 */
public class RemotingSendRequestException extends RemotingException {

    public RemotingSendRequestException(String message) {
        this(message, null);
    }

    public RemotingSendRequestException(String address, Throwable cause) {
        super("send request to " + address + " failed", cause);
    }
}
