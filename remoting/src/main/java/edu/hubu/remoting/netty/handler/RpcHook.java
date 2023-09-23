package edu.hubu.remoting.netty.handler;

/**
 * @author: sugar
 * @date: 2023/5/19
 * @description:
 */
public interface RpcHook {

    void doBeforeHook();

    void doAfterHook();

}
