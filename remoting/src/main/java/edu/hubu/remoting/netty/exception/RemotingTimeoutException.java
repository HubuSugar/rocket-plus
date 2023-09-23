package edu.hubu.remoting.netty.exception;

/**
 * @author: sugar
 * @date: 2023/5/27
 * @description:
 */
public class RemotingTimeoutException extends RemotingException {
    public RemotingTimeoutException(String message) {
        super(message);
    }

    public RemotingTimeoutException(String address, long timeout) {
        this(address, timeout, null);
    }

    public RemotingTimeoutException(String message, long timeout, Throwable cause) {
        super(String.format("wait response on channel <%s> timeout, {%d}ms", message, timeout), cause);
    }
}
