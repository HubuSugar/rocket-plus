package edu.hubu.store.lock;

import java.util.concurrent.locks.ReentrantLock;

/**
 * @author: sugar
 * @date: 2023/7/16
 * @description: exclusive lock implements to put message
 */
public class PutMessageReentrantLock implements PutMessageLock{
    private final ReentrantLock lock = new ReentrantLock();
    @Override
    public void lock() {
        lock.lock();
    }

    @Override
    public void unlock() {
        lock.unlock();
    }
}
