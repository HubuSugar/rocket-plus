package edu.hubu.store.lock;

/**
 * @author: sugar
 * @date: 2023/7/16
 * @description:
 */
public interface PutMessageLock {
    void lock();

    void unlock();
}
