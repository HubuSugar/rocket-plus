package edu.hubu.store;

import edu.hubu.common.BrokerConfig;
import edu.hubu.common.ServiceThread;
import edu.hubu.common.SystemClock;
import edu.hubu.common.message.MessageExtBrokerInner;
import edu.hubu.common.sysFlag.MessageSysFlag;
import edu.hubu.common.topic.TopicValidator;
import edu.hubu.common.utils.MixAll;
import edu.hubu.store.config.BrokerRole;
import edu.hubu.store.config.MessageStoreConfig;
import edu.hubu.store.config.StorePathConfigHelper;
import edu.hubu.store.consumeQueue.CommitLogDispatcher;
import edu.hubu.store.consumeQueue.DispatchRequest;
import edu.hubu.store.dledger.DLedgerCommitLog;
import edu.hubu.store.index.IndexService;
import edu.hubu.store.listen.MessageArrivingListener;
import edu.hubu.store.schedule.ScheduleMessageService;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileLock;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author: sugar
 * @date: 2023/7/15
 * @description:
 */
@Slf4j
public class DefaultMessageStore implements MessageStore {

    private final BrokerConfig brokerConfig;
    private final MessageStoreConfig messageStoreConfig;
    private final MessageArrivingListener messageArriveListener;

    private final CommitLog commitLog;

    private final ScheduleMessageService scheduleMessageService;

    private TransientStorePool transientStorePool;

    //<topic, <queueId, ConsumeQueue>>
    private final ConcurrentHashMap<String, ConcurrentHashMap<Integer, ConsumeQueue>> consumeQueueTable;

    private final FlushConsumeQueueService flushConsumeQueueService;
    private final IndexService indexService;

    private final AllocateMappedFileService allocateMappedFileService;

    private final ReputMessageService reputMessageService;

    //当前时间
    private final SystemClock systemClock = new SystemClock();

    private volatile boolean shutdown = false;

    //dispatcher
    private final LinkedList<CommitLogDispatcher> dispatcherList;

    private RandomAccessFile lockFile;
    private FileLock lock;
    private boolean shutdownNormal = false;

    private StoreCheckpoint storeCheckpoint;

    private final RunningFlags runningFlags = new RunningFlags();

    //在brokerController创建DefaultMessageStore对象
    public DefaultMessageStore(BrokerConfig brokerConfig, MessageStoreConfig messageStoreConfig, MessageArrivingListener messageArriveListener) throws FileNotFoundException {
        this.brokerConfig = brokerConfig;
        this.messageStoreConfig = messageStoreConfig;
        this.messageArriveListener = messageArriveListener;

        this.allocateMappedFileService = new AllocateMappedFileService(this);
        if (messageStoreConfig.isEnableDLedgerCommitlog()) {
            this.commitLog = new DLedgerCommitLog(this);
        } else {
            this.commitLog = new CommitLog(this);
        }

        //buildConsumeQueue、flushConsumeQueue
        this.consumeQueueTable = new ConcurrentHashMap<>(32);
        this.flushConsumeQueueService = new FlushConsumeQueueService();

        //indexService
        this.indexService = new IndexService(this);

        //haService

        //reputService
        this.reputMessageService = new ReputMessageService();

        this.scheduleMessageService = new ScheduleMessageService(this);

        //transientPool
        this.transientStorePool = new TransientStorePool(messageStoreConfig);
        if (messageStoreConfig.isEnableTransientStorePool()) {
            this.transientStorePool.init();  //初始化
        }

        //启动allocate mapped file 线程
        this.allocateMappedFileService.start();

        this.indexService.start();

        //初始化dispatcher
        this.dispatcherList = new LinkedList<>();
        this.dispatcherList.addLast(new CommitLogConsumeQueueDispatcher());
        this.dispatcherList.addLast(new CommitLogIndexDispatcher());

        //lock file
        File file = new File(StorePathConfigHelper.getLockFilePath(messageStoreConfig.getStorePathRootDir()));
        MappedFile.ensureDirOk(file.getParent());
        lockFile = new RandomAccessFile(file, "rw");
    }

    @Override
    public long now() {
        return System.currentTimeMillis();
    }

    @Override
    public boolean load() {
        boolean result = true;

        result = this.commitLog.load();

        result = result && this.loadConsumeQueue();

        this.storeCheckpoint = new StoreCheckpoint();
        return result;
    }

    //broker controller start()中启动
    public void start() throws Exception {
        lock = this.lockFile.getChannel().tryLock(0, 1, false);
        if (lock == null || lock.isShared() || !lock.isValid()) {
            throw new RuntimeException("lock failed, mq already started");
        }

        lockFile.getChannel().write(ByteBuffer.wrap("lock".getBytes(StandardCharsets.UTF_8)));
        lockFile.getChannel().force(true);

        //启动reputService
        long maxPhysicalLogicOffset = commitLog.getMinOffset();
        for (ConcurrentHashMap<Integer, ConsumeQueue> value : this.consumeQueueTable.values()) {
            for (ConsumeQueue cq : value.values()) {
                if (cq.getMaxPhysicOffset() > maxPhysicalLogicOffset) {
                    maxPhysicalLogicOffset = cq.getMaxPhysicOffset();
                }
            }
        }
        if (maxPhysicalLogicOffset < 0) {
            maxPhysicalLogicOffset = 0;
        }
        if (maxPhysicalLogicOffset < this.commitLog.getMinOffset()) {
            /**
             * 出现这种情况的原因：
             * 1、consumeQueue丢失或者人为删除
             * 2、启动了一个新的broker, 并且从其他broker复制了consumeQueue文件
             *
             * 但是这些情况的共同点是， maxPhysicalOffset为0， 如果不是那么应该是出现了一些异常情况
             */

            log.warn("Too small consumeQueue offset, maxPhysicalLoginOffset={}, commitlogMinOffset={}", maxPhysicalLogicOffset, this.commitLog.getMinOffset());
            maxPhysicalLogicOffset = this.commitLog.getMinOffset();
        }
        log.info("【setReputOffset】maxPhysicalOffset= {}, commitlogMinOffset = {}, commitlogMaxOffset={}, commitlogConfirmOffset = {}", maxPhysicalLogicOffset,
                this.commitLog.getMinOffset(), this.commitLog.getMaxOffset(), this.commitLog.getConfirmOffset());
        this.reputMessageService.setReputFromOffset(maxPhysicalLogicOffset);
        this.reputMessageService.start();

        //启动consumeQueue刷盘线程
        this.flushConsumeQueueService.start();
        //启动commitlog
        this.commitLog.start();
    }


    private boolean loadConsumeQueue() {
        File logicDir = new File(StorePathConfigHelper.getStorePathConsumeQueue(this.messageStoreConfig.getStorePathRootDir()));
        File[] logicTopicFiles = logicDir.listFiles();
        if (logicTopicFiles != null) {
            for (File topicFile : logicTopicFiles) {
                String topicName = topicFile.getName();
                File[] queueFiles = topicFile.listFiles();
                if (queueFiles != null) {
                    for (File queueFile : queueFiles) {
                        int queueId;
                        try {
                            queueId = Integer.parseInt(queueFile.getName());
                        } catch (NumberFormatException e) {
                            continue;
                        }
                        ConsumeQueue consumeQueue = new ConsumeQueue(topicName, queueId, StorePathConfigHelper.getStorePathConsumeQueue(this.messageStoreConfig.getStorePathRootDir()),
                                this.messageStoreConfig.getMappedFileSizeConsumeQueue(), this);
                        this.putConsumeQueue(topicName, queueId, consumeQueue);
                        if (!consumeQueue.load()) {
                            return false;
                        }
                    }
                }
            }
        }

        log.info("load consume queue all over, OK");

        return true;
    }


    @Override
    public CompletableFuture<PutMessageResult> asyncPutMessage(MessageExtBrokerInner message) {
        //1、检查存储的状态
        PutMessageStatus putMessageStatus = this.checkStoreStatus();

        //2、消息的合法性检查 topic的长度、属性的大小
        PutMessageStatus putMessageStatus1 = this.checkMessage();

        //3、将消息拼接到commitlog
        CompletableFuture<PutMessageResult> putMessageResult = this.commitLog.asyncPutMessage(message);


        return putMessageResult;
    }


    @Override
    public PutMessageResult putMessage(MessageExtBrokerInner message) {
        return null;
    }

    @Override
    public void cleanExpiredConsumeQueue() {
        long minOffset = this.commitLog.getMinOffset();
        //<topic, <queueId, ConsumeQueue>>
        Iterator<Map.Entry<String, ConcurrentHashMap<Integer, ConsumeQueue>>> iterator = this.consumeQueueTable.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, ConcurrentHashMap<Integer, ConsumeQueue>> next = iterator.next();
            String topic = next.getKey();
            if (!topic.equals(TopicValidator.RMQ_SYS_SCHEDULE_TOPIC)) {
                ConcurrentHashMap<Integer, ConsumeQueue> queueTable = next.getValue();
                Iterator<Map.Entry<Integer, ConsumeQueue>> qtIt = queueTable.entrySet().iterator();
                while (qtIt.hasNext()) {
                    Map.Entry<Integer, ConsumeQueue> nextQt = qtIt.next();
                    //获取consumeQueue的最后offset
                    // long cqLastOffset = nextQt.getValue().getLastOffset();
                    // if(cqLastOffset == -1){
                    //     log.warn("");
                    // }else if(cqLastOffset < minOffset){
                    //
                    //     qtIt.remove();
                    // }

                }

                if (queueTable.isEmpty()) {
                    log.info("cleanExpiredConsumeQueue: {},topic destroyed", topic);
                    iterator.remove();
                }
            }
        }
    }

    @Override
    public long getMaxOffsetInQueue(String topic, Integer queueId) {
        return 0;
    }

    @Override
    public GetMessageResult getMessage(String consumerGroup, String topic, Integer queueId,
                                       Long offset, Integer maxMsgNums, MessageFilter messageFilter) {
        if (shutdown) {
            log.warn("the message store has shutdown, so getMessage is forbidden");
            return null;
        }
        if (!this.runningFlags.isReadable()) {
            log.warn("the message store is not readable, so getMessage is forbidden, runningFlag:{}", this.runningFlags.getFlagBits());
            return null;
        }

        long beginTime = System.currentTimeMillis();
        GetMessageStatus status = GetMessageStatus.NO_MESSAGE_IN_QUEUE;
        long nextBeginOffset = offset;
        long minOffset = 0;
        long maxOffset = 0;

        GetMessageResult getMessageResult = new GetMessageResult();

        long maxOffsetPy = this.commitLog.getMaxOffset();
        ConsumeQueue consumeQueue = findConsumeQueue(topic, queueId);
        if (consumeQueue == null) {
            status = GetMessageStatus.NO_MATCHED_LOGIC_QUEUE;
            nextBeginOffset = nextOffsetCorrection(offset, 0);
        } else {
            minOffset = consumeQueue.getMinOffsetInQueue();
            maxOffset = consumeQueue.getMaxOffsetInQueue();

            if (maxOffset == 0) {
                status = GetMessageStatus.NO_MESSAGE_IN_QUEUE;
                nextBeginOffset = nextOffsetCorrection(offset, 0);
            } else if (offset < minOffset) {
                status = GetMessageStatus.OFFSET_TOO_SMALL;
                nextBeginOffset = nextOffsetCorrection(offset, minOffset);
            } else if (offset == maxOffset) {
                status = GetMessageStatus.OFFSET_OVERFLOW_ONE;
                nextBeginOffset = nextOffsetCorrection(offset, offset);
            } else if (offset > maxOffset) {
                status = GetMessageStatus.OFFSET_OVERFLOW_BADLY;
                if (minOffset == 0) {
                    nextBeginOffset = nextOffsetCorrection(offset, minOffset);
                } else {
                    nextBeginOffset = nextOffsetCorrection(offset, maxOffset);
                }
            } else {
                SelectMappedBufferResult bufferConsumeQueue = consumeQueue.getIndexBuffer(offset);
                if (bufferConsumeQueue == null) {
                    status = GetMessageStatus.OFFSET_FOUND_NULL;
                    nextBeginOffset = nextOffsetCorrection(offset, consumeQueue.rollNextFile(offset));
                } else {
                    try{
                        status = GetMessageStatus.NO_MATCHED_MSG;

                        long nextPhyFileStartOffset = Long.MIN_VALUE;
                        long maxPhyOffsetPulling = 0;

                        final int maxFilterMsgCount = Math.max(16000, maxMsgNums * ConsumeQueue.CONSUME_QUEUE_UNIT_SIZE);
                        final boolean diskFallRecorded = this.messageStoreConfig.isDiskFallRecorded();
                        int i = 0;
                        ConsumeQueueExt.CqUnitExt cqUnitExt = new ConsumeQueueExt.CqUnitExt();
                        for (; i < bufferConsumeQueue.getSize() && i < maxFilterMsgCount; i += ConsumeQueue.CONSUME_QUEUE_UNIT_SIZE) {
                            long offsetPy = bufferConsumeQueue.getByteBuffer().getLong();
                            int sizePy = bufferConsumeQueue.getByteBuffer().getInt();
                            long tagsCode = bufferConsumeQueue.getByteBuffer().getLong();

                            maxPhyOffsetPulling = sizePy;

                            if (nextPhyFileStartOffset != Long.MIN_VALUE) {
                                if(offsetPy < nextPhyFileStartOffset){
                                    continue;
                                }
                            }

                            boolean isInDisk = checkInDiskByCommitOffset(offsetPy, maxOffsetPy);

                            if (this.isTheBatchFull(sizePy, maxMsgNums, getMessageResult.getBufferTotalSize(), getMessageResult.getMessageCount(), isInDisk)) {
                                break;
                            }

                            boolean extRet = false, isTagsCodeLegal = true;
                            if (consumeQueue.isExtAddr(tagsCode)) {
                                extRet = consumeQueue.getExt(tagsCode, cqUnitExt);
                                if (extRet) {
                                    tagsCode = cqUnitExt.getTagsCode();
                                } else {
                                    isTagsCodeLegal = false;
                                }
                            }

                            if (messageFilter != null &&
                                    !messageFilter.isMatchedByConsumeQueue(isTagsCodeLegal ? tagsCode : null, extRet ? cqUnitExt : null)) {
                                if (getMessageResult.getBufferTotalSize() == 0) {
                                    status = GetMessageStatus.NO_MATCHED_MSG;
                                }
                                continue;
                            }

                            SelectMappedBufferResult selectResult = this.commitLog.getMessage(offsetPy, sizePy);
                            if (selectResult == null) {
                                if(getMessageResult.getBufferTotalSize() == 0){
                                    status = GetMessageStatus.MSG_WAS_REMOVING;
                                }
                                nextPhyFileStartOffset = this.commitLog.rollNextFile(offset);
                                continue;
                            }

                            getMessageResult.addMessage(selectResult);
                            status = GetMessageStatus.FOUND;
                            nextPhyFileStartOffset = Long.MIN_VALUE;
                        }
                        //如果需要记录落后的记录
                        if(diskFallRecorded){
                            long fallBehind = maxOffsetPy - maxPhyOffsetPulling;
                            //统计
                        }

                        //
                        nextBeginOffset = offset + ( i / ConsumeQueue.CONSUME_QUEUE_UNIT_SIZE);
                        long diff = maxOffsetPy - maxPhyOffsetPulling;
                        long memory = (long) (StoreUtil.TOTAL_PHYSICAL_MEMORY_SIZE * (this.messageStoreConfig.getAccessMessageInMemoryMaxRatio() / 100.0));
                        getMessageResult.setSuggestPullingFromSlave(diff > memory);
                    }finally {
                        bufferConsumeQueue.release();
                    }
                }
            }
        } //consumeQueue != null;

        //增加统计
        if(status == GetMessageStatus.FOUND){

        }

        getMessageResult.setStatus(status);
        getMessageResult.setNextBeginOffset(nextBeginOffset);
        getMessageResult.setMinOffset(minOffset);
        getMessageResult.setMaxOffset(maxOffset);

        return getMessageResult;
    }

    private boolean isTheBatchFull(int sizePy, int maxMsgNums, int bufferTotal, int messageTotal, boolean isInDisk) {
        if(bufferTotal == 0 || messageTotal == 0){
            return false;
        }
        if(maxMsgNums <= messageTotal){
            return true;
        }

        if(isInDisk){
            if((sizePy + bufferTotal) > this.messageStoreConfig.getMaxTransferBytesOnMessageInDisk()){
                return true;
            }

            if(messageTotal > this.messageStoreConfig.getMaxTransferCountOnMessageInDisk() - 1){
                return true;
            }
        }else{
            if((sizePy + bufferTotal) > this.messageStoreConfig.getMaxTransferBytesOnMessageInMemory()){
                return true;
            }

            if(messageTotal > this.messageStoreConfig.getMaxTransferCountOnMessageInMemory() - 1){
                return true;
            }
        }
        return false;
    }

    private boolean checkInDiskByCommitOffset(long offsetPy, long maxOffsetPy) {
        long memory = (long)(StoreUtil.TOTAL_PHYSICAL_MEMORY_SIZE * (this.messageStoreConfig.getAccessMessageInMemoryMaxRatio() / 100.00));
        return (maxOffsetPy - offsetPy) > memory;
    }

    private long nextOffsetCorrection(long oldOffset, long newOffset) {
        long nextOffset = oldOffset;
        if (this.getMessageStoreConfig().getBrokerRole() != BrokerRole.SLAVE || this.getMessageStoreConfig().isCheckOffsetInSlave()) {
            nextOffset = newOffset;
        }
        return newOffset;
    }

    public void doDispatch(DispatchRequest request) {
        for (CommitLogDispatcher dispatcher : this.dispatcherList) {
            dispatcher.dispatch(request);
        }
    }

    /**
     * 创建consumeQueue
     *
     * @param request
     */
    public void putMessagePositionInfo(DispatchRequest request) {
        ConsumeQueue consumeQueue = this.findConsumeQueue(request.getTopic(), request.getQueueId());
        consumeQueue.putMessagePositionInfoWrapper(request);
    }

    /**
     * 根据topic和queueId找到对应的consumeQueue
     *
     * @param topic   topic
     * @param queueId queueId
     * @return
     */
    public ConsumeQueue findConsumeQueue(final String topic, final int queueId) {
        ConcurrentHashMap<Integer, ConsumeQueue> map = this.consumeQueueTable.get(topic);
        if (map == null) {
            ConcurrentHashMap<Integer, ConsumeQueue> newMap = new ConcurrentHashMap<>(128);
            ConcurrentHashMap<Integer, ConsumeQueue> oldMap = this.consumeQueueTable.put(topic, newMap);
            if (oldMap != null) {
                map = oldMap;
            } else {
                map = newMap;
            }
        }

        ConsumeQueue logic = map.get(queueId);

        if (logic != null) {
            return logic;
        }

        ConsumeQueue newLogic = new ConsumeQueue(topic, queueId, StorePathConfigHelper.getStorePathConsumeQueue(messageStoreConfig.getStorePathRootDir()), messageStoreConfig.getMappedFileSizeConsumeQueue(),
                this);
        ConsumeQueue oldLogic = map.putIfAbsent(queueId, newLogic);
        if (oldLogic == null) {
            logic = newLogic;
        } else {
            logic = oldLogic;
        }

        return logic;
    }


    public void unlockMappedFile(MappedFile unlockMappedFile) {

    }

    private PutMessageStatus checkStoreStatus() {
        if (shutdown) {
            return PutMessageStatus.SERVICE_NOT_AVAILABLE;
        }

        return null;
    }

    private PutMessageStatus checkMessage() {
        return null;
    }

    /**
     * 启动时加载consumeQueue
     *
     * @param topic
     * @param queueId
     * @param consumeQueue
     */
    public void putConsumeQueue(String topic, int queueId, ConsumeQueue consumeQueue) {
        ConcurrentHashMap<Integer, ConsumeQueue> queueMap = this.consumeQueueTable.get(topic);
        if (queueMap == null) {
            queueMap = new ConcurrentHashMap<>();
            queueMap.put(queueId, consumeQueue);
            this.consumeQueueTable.put(topic, queueMap);
        } else {
            queueMap.put(queueId, consumeQueue);
        }
    }

    public BrokerConfig getBrokerConfig() {
        return brokerConfig;
    }

    public MessageStoreConfig getMessageStoreConfig() {
        return messageStoreConfig;
    }

    public MessageArrivingListener getMessageArriveListener() {
        return messageArriveListener;
    }

    public ConcurrentHashMap<String, ConcurrentHashMap<Integer, ConsumeQueue>> getConsumeQueueTable() {
        return consumeQueueTable;
    }

    public AllocateMappedFileService getAllocateMappedFileService() {
        return allocateMappedFileService;
    }

    public SystemClock getSystemClock() {
        return systemClock;
    }

    public TransientStorePool getTransientStorePool() {
        return transientStorePool;
    }

    public void setTransientStorePool(TransientStorePool transientStorePool) {
        this.transientStorePool = transientStorePool;
    }

    public StoreCheckpoint getStoreCheckpoint() {
        return storeCheckpoint;
    }

    public void setStoreCheckpoint(StoreCheckpoint storeCheckpoint) {
        this.storeCheckpoint = storeCheckpoint;
    }

    public RunningFlags getRunningFlags() {
        return runningFlags;
    }

    public ScheduleMessageService getScheduleMessageService() {
        return scheduleMessageService;
    }

    /**
     * 构建consume Queue
     */
    class CommitLogConsumeQueueDispatcher implements CommitLogDispatcher {
        @Override
        public void dispatch(DispatchRequest request) {
            int tranType = MessageSysFlag.getTransactionValue(request.getSysFlag());
            switch (tranType) {
                case MessageSysFlag.TRANSACTION_NOT_TYPE:
                case MessageSysFlag.TRANSACTION_COMMIT_TYPE:
                    //dispatch时创建consumeQueue
                    DefaultMessageStore.this.putMessagePositionInfo(request);
                    break;
                case MessageSysFlag.TRANSACTION_PREPARE_TYPE:
                case MessageSysFlag.TRANSACTION_ROLLBACK_TYPE:
                    break;
            }
        }
    }

    /**
     * 构建index
     */
    class CommitLogIndexDispatcher implements CommitLogDispatcher {
        @Override
        public void dispatch(DispatchRequest request) {
            if (DefaultMessageStore.this.messageStoreConfig.isMessageIndexEnable()) {
                DefaultMessageStore.this.indexService.buildIndex(request);
            }
        }
    }

    /**
     * 清除过期的 CommitLog
     */
    class CleanCommitLogService {

    }

    /**
     * 清除过期的ConsumeQueue
     */
    class CleanConsumeQueueService {

    }

    /**
     * 进行consumeQueue刷盘
     */
    class FlushConsumeQueueService extends ServiceThread {
        private static final int RETRY_TIMES_OVER = 3;

        private long lastFlushTimestamp = 0;

        @Override
        public String getServiceName() {
            return FlushConsumeQueueService.class.getSimpleName();
        }

        @Override
        public void run() {
            DefaultMessageStore.log.info(getServiceName() + " service started");
            while (!isStopped()) {
                try {
                    int interval = DefaultMessageStore.this.getMessageStoreConfig().getFlushIntervalConsumeQueue();
                    this.waitForRunning(interval);
                    this.doFlush(1);
                } catch (Exception e) {
                    log.error(getServiceName() + " flush consume queue has exception", e);
                }
            }

            this.doFlush(RETRY_TIMES_OVER);

            DefaultMessageStore.log.info(getServiceName() + "service stopped");
        }

        /**
         * consume Queue刷盘逻辑
         *
         * @param retryTimes
         */
        private void doFlush(int retryTimes) {
            int flushConsumeQueuePages = DefaultMessageStore.this.getMessageStoreConfig().getFlushConsumeQueueLeastPages();
            if (retryTimes == RETRY_TIMES_OVER) {
                flushConsumeQueuePages = 0;
            }

            long logicMsgTimestamp = 0;

            int flushConsumeQueueThoroughInterval = DefaultMessageStore.this.getMessageStoreConfig().getFlushConsumeQueueThoroughInterval();
            long now = System.currentTimeMillis();
            if (now >= (this.lastFlushTimestamp + flushConsumeQueueThoroughInterval)) {
                //说明需要进行刷盘操作
                this.lastFlushTimestamp = now;
                flushConsumeQueuePages = 0;
                logicMsgTimestamp = DefaultMessageStore.this.getStoreCheckpoint().getLogicMsgTimestamp();
            }

            for (ConcurrentHashMap<Integer, ConsumeQueue> maps : DefaultMessageStore.this.consumeQueueTable.values()) {
                for (ConsumeQueue cq : maps.values()) {
                    boolean result = false;
                    for (int i = 0; i < retryTimes && !result; i++) {
                        result = cq.flush(flushConsumeQueuePages);
                    }
                }
            }

            if (0 == flushConsumeQueuePages) {
                if (logicMsgTimestamp > 0) {
                    DefaultMessageStore.this.getStoreCheckpoint().setLogicMsgTimestamp(logicMsgTimestamp);
                }
                DefaultMessageStore.this.getStoreCheckpoint().flush();
            }
        }

        @Override
        public long getJoinTime() {
            return 1000 * 60;
        }
    }

    /**
     * 对消息进行dispatcher
     */
    class ReputMessageService extends ServiceThread {
        private volatile long reputFromOffset;

        @Override
        public String getServiceName() {
            return ReputMessageService.class.getSimpleName();
        }

        @Override
        public void run() {
            log.info(getServiceName() + " service started");
            while (!isStopped()) {
                try {
                    Thread.sleep(1);
                    this.doReput();
                } catch (InterruptedException e) {
                    log.error(getServiceName() + " service has exception");
                }
            }
            log.info(getServiceName() + " service stopped");
        }

        public long behind() {
            return DefaultMessageStore.this.commitLog.getMaxOffset() - reputFromOffset;
        }

        private boolean isCommitLogAvailable() {
            return behind() > 0;
        }

        private void doReput() {
            //reput位点小于最小偏移量，说明reput积压了很多消息
            if (this.reputFromOffset < DefaultMessageStore.this.commitLog.getMinOffset()) {
                log.warn("the reputFromOffset = {} is smaller then the minPyOffset= {}, this usually indicate the dispatch behind too much and the commit log has expired.", this.reputFromOffset,
                        DefaultMessageStore.this.commitLog.getMinOffset());
                this.reputFromOffset = DefaultMessageStore.this.commitLog.getMinOffset();
            }
            for (boolean doNext = true; this.isCommitLogAvailable() && doNext; ) {
                if (DefaultMessageStore.this.messageStoreConfig.isDuplicateEnable()
                        && this.reputFromOffset >= DefaultMessageStore.this.commitLog.getConfirmOffset()) {
                    break;
                }
                //获取将要reput的ByteBuffer
                SelectMappedBufferResult result = DefaultMessageStore.this.commitLog.getData(reputFromOffset);
                if (result != null) {
                    try {
                        this.reputFromOffset = result.getStartOffset();

                        for (int readSize = 0; readSize < result.getSize() && doNext; ) {
                            DispatchRequest request = DefaultMessageStore.this.commitLog.checkMessageAndReturnSize(result.getByteBuffer(), false, false);
                            int size = request.getBufferSize() == -1 ? request.getMsgSize() : request.getBufferSize();

                            if (request.isSuccess()) {
                                if (size > 0) {
                                    //进行消息分发到consumeQueue
                                    DefaultMessageStore.this.doDispatch(request);
                                    //如果主节点开启了长轮询, 有新消息时回调通知
                                    if (BrokerRole.SLAVE != DefaultMessageStore.this.getMessageStoreConfig().getBrokerRole()
                                            && DefaultMessageStore.this.getBrokerConfig().isLongPollingEnable()) {
                                        DefaultMessageStore.this.messageArriveListener.arriving(request.getTopic(), request.getQueueId(),
                                                request.getConsumeQueueOffset() + 1, request.getTagsCode(), request.getStoreTimestamp(),
                                                request.getBitMap(), request.getPropertiesMap());
                                    }

                                    this.reputFromOffset += size;
                                    readSize += size;

                                    if (DefaultMessageStore.this.getMessageStoreConfig().getBrokerRole() == BrokerRole.SLAVE) {
                                        //执行统计
                                    }
                                } else if (size == 0) {
                                    this.reputFromOffset = DefaultMessageStore.this.commitLog.rollNextFile(this.reputFromOffset);
                                    readSize = result.getSize();
                                }
                            } else if (!request.isSuccess()) {
                                if (size > 0) {
                                    this.reputFromOffset += size;
                                } else {
                                    doNext = false;
                                    //如果打开了dledger模式或者broker是主节点，那么不能忽略异常，并且需要需要reputFromOffset变量
                                    if (DefaultMessageStore.this.getMessageStoreConfig().isEnableDLedgerCommitlog() ||
                                            DefaultMessageStore.this.getBrokerConfig().getBrokerId() == MixAll.MASTER_ID) {
                                        log.error("[BUG]dispatch message to consume queue error, COMMITLOG OFFSET:{}", this.reputFromOffset);
                                        this.reputFromOffset += size;
                                    }
                                }
                            }
                        }
                    } finally {
                        result.release();
                    }
                } else {
                    doNext = false;
                }
            }
        }

        public long getReputFromOffset() {
            return reputFromOffset;
        }

        public void setReputFromOffset(long reputFromOffset) {
            this.reputFromOffset = reputFromOffset;
        }
    }

}
