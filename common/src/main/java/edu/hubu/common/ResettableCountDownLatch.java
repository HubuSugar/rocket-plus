package edu.hubu.common;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.AbstractQueuedSynchronizer;

/**
 * @author: sugar
 * @date: 2023/8/5
 * @description:
 */
public class ResettableCountDownLatch {
    private final Sync sync;

    public ResettableCountDownLatch(int count){
        if(count < 0){
            throw new IllegalArgumentException("count must be greater than zero");
        }
        this.sync = new Sync(count);
    }

    public void countDown(){
        sync.releaseShared(1);
    }

    public void await() throws InterruptedException {
        sync.acquireSharedInterruptibly(1);
    }

    public boolean await(long timeout, TimeUnit unit) throws InterruptedException {
       return sync.tryAcquireSharedNanos(1, unit.toNanos(timeout));
    }

    public int getCount(){
        return sync.getCount();
    }

    public void reset(){
        sync.reset();
    }

    static final class Sync extends AbstractQueuedSynchronizer{
        private int count;

        public Sync(int count) {
            this.count = count;
            setState(count);
        }

        int getCount(){
            return getState();
        }

        protected int tryAcquireShared(int acquires){
            return getState() == 0 ? 1 : -1;
        }

        protected boolean tryReleaseShared(int releases){
            for (;;){
                int c = getState();
                if(c == 0){
                    return false;
                }
                int nextC = c - 1;
                if(compareAndSetState(c, nextC)){
                    return nextC == 0;
                }
            }
        }

        protected void reset(){
            setState(count);
        }
    }
}
