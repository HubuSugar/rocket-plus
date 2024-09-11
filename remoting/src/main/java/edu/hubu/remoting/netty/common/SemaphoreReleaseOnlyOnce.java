package edu.hubu.remoting.netty.common;

import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author: sugar
 * @date: 2024/9/10
 * @description:
 */
public class SemaphoreReleaseOnlyOnce {
    private final AtomicBoolean released = new AtomicBoolean(false);
    private final Semaphore semaphore;

    public SemaphoreReleaseOnlyOnce(Semaphore semaphore) {
        this.semaphore = semaphore;
    }

    public void release(){
        if(this.semaphore != null){
            if(this.released.compareAndSet(false, true)){
              this.semaphore.release();
            }
        }
    }
}
