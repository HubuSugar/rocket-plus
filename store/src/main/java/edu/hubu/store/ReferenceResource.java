package edu.hubu.store;


import java.util.concurrent.atomic.AtomicLong;

/**
 * @author: sugar
 * @date: 2023/8/13
 * @description:
 */
public abstract class ReferenceResource {

    protected final AtomicLong refCount = new AtomicLong(1);
    protected volatile boolean available = true;
    protected volatile boolean cleanupOver = false;
    private volatile long firstShutdownTimestamp = 0;

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
        long refCount = this.refCount.decrementAndGet();
        if(refCount > 0) return;
        synchronized (this){
            this.cleanupOver = this.cleanup(refCount);
        }
    }

    public abstract boolean cleanup(long refCount);

    public boolean isCleanupOver() {
        return this.refCount.get() <= 0 && this.cleanupOver;
    }

    public void setCleanupOver(boolean cleanupOver) {
        this.cleanupOver = cleanupOver;
    }

    protected void shutdown(final long intervalForcibly){
        if(this.available){
            this.available = false;
            this.firstShutdownTimestamp = System.currentTimeMillis();
            this.release();
        }else if(this.getRefCount() > 0){
            if(System.currentTimeMillis() - firstShutdownTimestamp >= intervalForcibly){
                this.refCount.set(-1000 - this.getRefCount());
                this.release();
            }

        }

    }

    public long getRefCount(){
       return this.refCount.get();
    }

}
