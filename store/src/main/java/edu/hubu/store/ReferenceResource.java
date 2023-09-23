package edu.hubu.store;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author: sugar
 * @date: 2023/8/13
 * @description:
 */
public abstract class ReferenceResource {

    protected final AtomicInteger refCount = new AtomicInteger(1);
    protected volatile boolean available = true;
    protected volatile boolean cleanupOver = false;

    /**
     * 是否还在被引用状态
     * @return result
     */
    public synchronized boolean isHold(){
        if(this.isAvailable()){
            if(this.refCount.getAndIncrement() > 0){
                return true;
            }else {
                this.refCount.getAndIncrement();
            }
        }
        return false;
    }

    public boolean isAvailable(){
        return this.available;
    }


    public void release(){
        int refCount = this.refCount.decrementAndGet();
        if(refCount > 0) return;
        synchronized (this){
            this.cleanupOver = this.cleanup(refCount);
        }
    }

    public abstract boolean cleanup(int refCount);

    public boolean isCleanupOver() {
        return this.refCount.get() <= 0 && this.cleanupOver;
    }

    public void setCleanupOver(boolean cleanupOver) {
        this.cleanupOver = cleanupOver;
    }
}
