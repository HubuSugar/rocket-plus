package edu.hubu.store.consumeQueue;

/**
 * @author: sugar
 * @date: 2023/8/19
 * @description:
 */
public interface CommitLogDispatcher {
    void dispatch(DispatchRequest request);
}
