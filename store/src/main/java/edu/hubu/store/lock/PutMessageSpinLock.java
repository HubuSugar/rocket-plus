package edu.hubu.store.lock;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author: sugar
 * @date: 2023/7/16
 * @description: 使用原子变量实现自旋锁
 */
public class PutMessageSpinLock implements PutMessageLock{
    private final AtomicBoolean lock = new AtomicBoolean(true);

    @Override
    public void lock() {
        boolean flag;
        do{
            flag = lock.compareAndSet(true, false);
        }while (!flag);
    }

    @Override
    public void unlock() {
        this.lock.compareAndSet(false, true);
    }
}
