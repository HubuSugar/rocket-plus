package edu.hubu.store.config;

import edu.hubu.store.ConsumeQueue;

import java.io.File;

/**
 * @author: sugar
 * @date: 2023/7/15
 * @description:
 */
public class MessageStoreConfig {

    // 文件存储的根路径
    private String storePathRootDir = System.getProperty("user.home") + File.separator + "store_plus";
    //commitlog存储路径
    private String commitlogStorePath = System.getProperty("user.home") + File.separator + "store_plus" + File.separator + "commitlog";
    //每个commitlog文件的分片大小，默认为1G，commitlog中mappedFile文件的大小
    private int mappedFileSizeCommitlog = 1024  * 4;
    //默认 300000 * 20
    private int mappedFileSizeConsumeQueue = 30 * ConsumeQueue.CONSUME_QUEUE_UNIT_SIZE;
    //是否开启dledger
    private boolean enableDLedgerCommitlog;
    //put message时选择锁的类型
    private boolean useReentrantLockWhenPutMessage = false;
    //是否开启transientStorePool
    private boolean enableTransientStorePool = false;
    private int transientStorePoolSize = 5;
    private boolean fastFailIfNoBufferInPool = false;

    private BrokerRole brokerRole = BrokerRole.ASYNC_MASTER;
    //是否预写入MappedFile，开启预热
    private boolean warmupMappedFileEnable;
    private FlushDiskType flushDiskType = FlushDiskType.SYNC_FLUSH;
    //flush least page when the disk in warm state
    private int flushLeastPagesWhenWarmup = 1024 / 4 * 16;
    //the max length of the message default 4M
    private int maxMessageSize = 1024 * 1024 * 4;
    //同步刷盘的超时时间
    private long syncFlushTimeout = 1000 * 5;
    private boolean messageIndexEnable = true;
    private boolean duplicateEnable = false;
    //每次刷盘consumeQueue的页数
    private int flushConsumeQueueLeastPages = 2;
    //consumeQueue的刷盘时间间隔
    private int flushConsumeQueueThoroughInterval = 1000 * 60;
    //consumeQueue刷盘阻塞时间
    private int flushIntervalConsumeQueue = 1000;
    private boolean enableConsumeQueueExt = false;

    // default 48m 48 * 1024 * 1024
    private int mappedFileSizeConsumeQueueExt = 48 * 1024 * 1024;
    private int bitmapLengthConsumeQueueExt = 64;
    private boolean checkOffsetInSlave = false;
    private boolean diskFallRecorded = true;
    private long accessMessageInMemoryMaxRatio = 40;
    private int maxTransferBytesOnMessageInMemory = 1024 * 256;
    private int maxTransferCountOnMessageInMemory = 32;
    private int maxTransferBytesOnMessageInDisk = 1024 * 64;
    private int maxTransferCountOnMessageInDisk = 8;
    private boolean offsetCheckInSlave = false;


    public String getStorePathRootDir() {
        return storePathRootDir;
    }

    public void setStorePathRootDir(String storePathRootDir) {
        this.storePathRootDir = storePathRootDir;
    }

    public String getCommitlogStorePath() {
        return commitlogStorePath;
    }

    public void setCommitlogStorePath(String commitlogStorePath) {
        this.commitlogStorePath = commitlogStorePath;
    }

    public boolean isEnableDLedgerCommitlog() {
        return enableDLedgerCommitlog;
    }

    public void setEnableDLedgerCommitlog(boolean enableDLedgerCommitlog) {
        this.enableDLedgerCommitlog = enableDLedgerCommitlog;
    }

    public boolean isUseReentrantLockWhenPutMessage() {
        return useReentrantLockWhenPutMessage;
    }

    public void setUseReentrantLockWhenPutMessage(boolean useReentrantLockWhenPutMessage) {
        this.useReentrantLockWhenPutMessage = useReentrantLockWhenPutMessage;
    }

    public boolean isEnableTransientStorePool() {
        return enableTransientStorePool;
    }

    public void setEnableTransientStorePool(boolean enableTransientStorePool) {
        this.enableTransientStorePool = enableTransientStorePool;
    }

    public boolean isFastFailIfNoBufferInPool() {
        return fastFailIfNoBufferInPool;
    }

    public void setFastFailIfNoBufferInPool(boolean fastFailIfNoBufferInPool) {
        this.fastFailIfNoBufferInPool = fastFailIfNoBufferInPool;
    }

    public BrokerRole getBrokerRole() {
        return brokerRole;
    }

    public void setBrokerRole(BrokerRole brokerRole) {
        this.brokerRole = brokerRole;
    }

    public int getMappedFileSizeCommitlog() {
        return mappedFileSizeCommitlog;
    }

    public void setMappedFileSizeCommitlog(int mappedFileSizeCommitlog) {
        this.mappedFileSizeCommitlog = mappedFileSizeCommitlog;
    }

    public int getTransientStorePoolSize() {
        return transientStorePoolSize;
    }

    public void setTransientStorePoolSize(int transientStorePoolSize) {
        this.transientStorePoolSize = transientStorePoolSize;
    }

    public boolean isWarmupMappedFileEnable() {
        return warmupMappedFileEnable;
    }

    public void setWarmupMappedFileEnable(boolean warmupMappedFileEnable) {
        this.warmupMappedFileEnable = warmupMappedFileEnable;
    }

    public FlushDiskType getFlushDiskType() {
        return flushDiskType;
    }

    public void setFlushDiskType(FlushDiskType flushDiskType) {
        this.flushDiskType = flushDiskType;
    }

    public int getFlushLeastPagesWhenWarmup() {
        return flushLeastPagesWhenWarmup;
    }

    public void setFlushLeastPagesWhenWarmup(int flushLeastPagesWhenWarmup) {
        this.flushLeastPagesWhenWarmup = flushLeastPagesWhenWarmup;
    }

    public int getMaxMessageSize() {
        return maxMessageSize;
    }

    public void setMaxMessageSize(int maxMessageSize) {
        this.maxMessageSize = maxMessageSize;
    }

    public long getSyncFlushTimeout() {
        return syncFlushTimeout;
    }

    public void setSyncFlushTimeout(long syncFlushTimeout) {
        this.syncFlushTimeout = syncFlushTimeout;
    }

    public boolean isMessageIndexEnable() {
        return messageIndexEnable;
    }

    public void setMessageIndexEnable(boolean messageIndexEnable) {
        this.messageIndexEnable = messageIndexEnable;
    }

    public boolean isDuplicateEnable() {
        return duplicateEnable;
    }

    public void setDuplicateEnable(boolean duplicateEnable) {
        this.duplicateEnable = duplicateEnable;
    }

    public int getFlushConsumeQueueLeastPages() {
        return flushConsumeQueueLeastPages;
    }

    public void setFlushConsumeQueueLeastPages(int flushConsumeQueueLeastPages) {
        this.flushConsumeQueueLeastPages = flushConsumeQueueLeastPages;
    }

    public int getFlushConsumeQueueThoroughInterval() {
        return flushConsumeQueueThoroughInterval;
    }

    public void setFlushConsumeQueueThoroughInterval(int flushConsumeQueueThoroughInterval) {
        this.flushConsumeQueueThoroughInterval = flushConsumeQueueThoroughInterval;
    }

    public int getFlushIntervalConsumeQueue() {
        return flushIntervalConsumeQueue;
    }

    public void setFlushIntervalConsumeQueue(int flushIntervalConsumeQueue) {
        this.flushIntervalConsumeQueue = flushIntervalConsumeQueue;
    }

    public boolean isEnableConsumeQueueExt() {
        return enableConsumeQueueExt;
    }

    public void setEnableConsumeQueueExt(boolean enableConsumeQueueExt) {
        this.enableConsumeQueueExt = enableConsumeQueueExt;
    }

    public int getMappedFileSizeConsumeQueue() {
        int factor = (int)Math.ceil(this.mappedFileSizeConsumeQueue / (ConsumeQueue.CONSUME_QUEUE_UNIT_SIZE * 1.0));
        return factor * ConsumeQueue.CONSUME_QUEUE_UNIT_SIZE;
    }

    public void setMappedFileSizeConsumeQueue(int mappedFileSizeConsumeQueue) {
        this.mappedFileSizeConsumeQueue = mappedFileSizeConsumeQueue;
    }

    public int getMappedFileSizeConsumeQueueExt() {
        return mappedFileSizeConsumeQueueExt;
    }

    public void setMappedFileSizeConsumeQueueExt(int mappedFileSizeConsumeQueueExt) {
        this.mappedFileSizeConsumeQueueExt = mappedFileSizeConsumeQueueExt;
    }

    public int getBitmapLengthConsumeQueueExt() {
        return bitmapLengthConsumeQueueExt;
    }

    public void setBitmapLengthConsumeQueueExt(int bitmapLengthConsumeQueueExt) {
        this.bitmapLengthConsumeQueueExt = bitmapLengthConsumeQueueExt;
    }

    public boolean isCheckOffsetInSlave() {
        return checkOffsetInSlave;
    }

    public void setCheckOffsetInSlave(boolean checkOffsetInSlave) {
        this.checkOffsetInSlave = checkOffsetInSlave;
    }

    public boolean isDiskFallRecorded() {
        return diskFallRecorded;
    }

    public void setDiskFallRecorded(boolean diskFallRecorded) {
        this.diskFallRecorded = diskFallRecorded;
    }

    public long getAccessMessageInMemoryMaxRatio() {
        return accessMessageInMemoryMaxRatio;
    }

    public void setAccessMessageInMemoryMaxRatio(long accessMessageInMemoryMaxRatio) {
        this.accessMessageInMemoryMaxRatio = accessMessageInMemoryMaxRatio;
    }

    public int getMaxTransferBytesOnMessageInDisk() {
        return maxTransferBytesOnMessageInDisk;
    }

    public void setMaxTransferBytesOnMessageInDisk(int maxTransferBytesOnMessageInDisk) {
        this.maxTransferBytesOnMessageInDisk = maxTransferBytesOnMessageInDisk;
    }

    public int getMaxTransferCountOnMessageInDisk() {
        return maxTransferCountOnMessageInDisk;
    }

    public void setMaxTransferCountOnMessageInDisk(int maxTransferCountOnMessageInDisk) {
        this.maxTransferCountOnMessageInDisk = maxTransferCountOnMessageInDisk;
    }

    public int getMaxTransferBytesOnMessageInMemory() {
        return maxTransferBytesOnMessageInMemory;
    }

    public void setMaxTransferBytesOnMessageInMemory(int maxTransferBytesOnMessageInMemory) {
        this.maxTransferBytesOnMessageInMemory = maxTransferBytesOnMessageInMemory;
    }

    public int getMaxTransferCountOnMessageInMemory() {
        return maxTransferCountOnMessageInMemory;
    }

    public void setMaxTransferCountOnMessageInMemory(int maxTransferCountOnMessageInMemory) {
        this.maxTransferCountOnMessageInMemory = maxTransferCountOnMessageInMemory;
    }

    public boolean isOffsetCheckInSlave() {
        return offsetCheckInSlave;
    }

    public void setOffsetCheckInSlave(boolean offsetCheckInSlave) {
        this.offsetCheckInSlave = offsetCheckInSlave;
    }
}
