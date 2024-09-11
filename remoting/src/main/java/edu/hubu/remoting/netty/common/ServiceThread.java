package edu.hubu.remoting.netty.common;

import lombok.extern.slf4j.Slf4j;

/**
 * @author: sugar
 * @date: 2024/8/18
 * @description:
 */
@Slf4j
public abstract class ServiceThread implements Runnable {

    private static final long JOIN_TIME = 90 * 1000;
    protected final Thread thread;
    protected volatile boolean hasNotified = false;
    protected volatile boolean stopped = false;

    public ServiceThread(){
        this.thread = new Thread(this, this.getServiceName());
    }

    public abstract String getServiceName();

    public void start(){
        this.thread.start();
    }

    public void shutdown(){
        shutdown(false);
    }

    public void shutdown(final boolean interrupted){
        this.stopped = true;
        synchronized (this){
            if(!this.hasNotified){
                this.hasNotified = true;
                this.notify();
            }
        }
        try{
            if(interrupted){
                this.thread.interrupt();
            }
            long beginTime = System.currentTimeMillis();
            this.thread.join(this.getJoinTime());
            long costTime = System.currentTimeMillis() - beginTime;
            log.info("join thread {} cost time {} ms", getServiceName(), costTime);
        } catch (InterruptedException e) {

        }

    }

    public long getJoinTime(){
        return JOIN_TIME;
    }

    public boolean isStopped(){
        return stopped;
    }


}
