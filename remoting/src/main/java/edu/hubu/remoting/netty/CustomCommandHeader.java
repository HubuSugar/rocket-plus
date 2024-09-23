package edu.hubu.remoting.netty;

import edu.hubu.remoting.netty.exception.RemotingCommandException;

/**
 * @author: sugar
 * @date: 2023/5/21
 * @description:
 */
public interface CustomCommandHeader {
    boolean checkFields() throws RemotingCommandException;
}
