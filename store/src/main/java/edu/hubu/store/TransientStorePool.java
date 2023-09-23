package edu.hubu.store;

import edu.hubu.store.config.MessageStoreConfig;

import java.nio.ByteBuffer;
import java.util.Deque;
import java.util.concurrent.ConcurrentLinkedDeque;

/**
 * @author: sugar
 * @date: 2023/7/16
 * @description:
 */
public class TransientStorePool {

    private int poolSize;
    private long fileSize;
    private final MessageStoreConfig storeConfig;
    private Deque<ByteBuffer> availableBuffers;

    public TransientStorePool(final MessageStoreConfig storeConfig) {
        this.storeConfig = storeConfig;
        this.poolSize = storeConfig.getTransientStorePoolSize();
        this.fileSize = storeConfig.getMappedFileSizeCommitlog();
        this.availableBuffers = new ConcurrentLinkedDeque<>();
    }

    /**
     * 初始化固定容量的byteBuffer, DirectByteBuffer
     */
    public void init(){


    }

    public int availableBufferNums(){
        if(this.storeConfig.isEnableTransientStorePool()){
            return availableBuffers.size();
        }
        return Integer.MAX_VALUE;
    }

    public ByteBuffer borrowBuffer(){
        return null;
    }

    public int getPoolSize() {
        return poolSize;
    }

    public void setPoolSize(int poolSize) {
        this.poolSize = poolSize;
    }

    public long getFileSize() {
        return fileSize;
    }

    public void setFileSize(long fileSize) {
        this.fileSize = fileSize;
    }
}
