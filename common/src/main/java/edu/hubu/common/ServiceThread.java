package edu.hubu.common;

import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author: sugar
 * @date: 2023/7/17
 * @description:
 */
@Slf4j
public abstract class ServiceThread implements Runnable{

    private static final long JOIN_TIME = 90 * 1000;
    private final AtomicBoolean started = new AtomicBoolean(false);
    private Thread thread;
    protected boolean isDaemon;
    private volatile boolean stopped = false;
    private final AtomicBoolean hasNotified = new AtomicBoolean(false);
    private final ResettableCountDownLatch countDownLatch = new ResettableCountDownLatch(1);

    public abstract String getServiceName();


    public void start(){
        if(!started.compareAndSet(false, true)){
            return;
        }
        this.thread = new Thread(this, this.getServiceName());
        thread.setDaemon(isDaemon);
        thread.start();
    }

    /**
     * 减少闭锁同步值
     */
    public void wakeup(){
        if(hasNotified.compareAndSet(false, true)){
            this.countDownLatch.countDown();
        }
    }

    public void waitForRunning(int interval){
        if(hasNotified.compareAndSet(true, false)){
            this.onWaitEnd();
            return;
        }

        this.countDownLatch.reset();

        try {
            countDownLatch.await(interval, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            log.error("interrupted", e);
        }finally {
            hasNotified.set(false);
            this.onWaitEnd();
        }
    }

    public void onWaitEnd(){

    }

    public long getJoinTime(){
        return JOIN_TIME;
    }

    public boolean isStopped() {
        return stopped;
    }

    public void setStopped(boolean stopped) {
        this.stopped = stopped;
    }
}
