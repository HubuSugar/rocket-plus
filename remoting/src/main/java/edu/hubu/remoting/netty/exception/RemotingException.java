package edu.hubu.remoting.netty.exception;

/**
 * @author: sugar
 * @date: 2023/5/27
 * @description:
 */
public class RemotingException extends Exception{

    public RemotingException(String message) {
        super(message);
    }

    public RemotingException(String message, Throwable cause) {
        super(message, cause);
    }
}
