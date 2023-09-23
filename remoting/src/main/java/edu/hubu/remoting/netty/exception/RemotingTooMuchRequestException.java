package edu.hubu.remoting.netty.exception;

/**
 * @author: sugar
 * @date: 2023/7/9
 * @description:
 */
public class RemotingTooMuchRequestException extends RemotingException{
    public RemotingTooMuchRequestException(String message) {
        super(message);
    }
}
