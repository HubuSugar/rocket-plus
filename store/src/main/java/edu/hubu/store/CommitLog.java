package edu.hubu.store;

import edu.hubu.common.ServiceThread;
import edu.hubu.common.message.*;
import edu.hubu.common.sysFlag.MessageSysFlag;
import edu.hubu.common.topic.TopicValidator;
import edu.hubu.common.utils.UtilAll;
import edu.hubu.store.config.BrokerRole;
import edu.hubu.store.config.FlushDiskType;
import edu.hubu.store.consumeQueue.DispatchRequest;
import edu.hubu.store.lock.PutMessageLock;
import edu.hubu.store.lock.PutMessageReentrantLock;
import edu.hubu.store.lock.PutMessageSpinLock;
import lombok.extern.slf4j.Slf4j;
import sun.security.krb5.internal.APRep;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * @author: sugar
 * @date: 2023/7/15
 * @description:
 */
@Slf4j
public class CommitLog {
    //message魔数
    public static final int MESSAGE_MAGIC_CODE = -626843481;
    //end of mapped file empty magic code cbd43194
    protected static final int BLANK_MAGIC_CODE = -875286124;
    //管理mappedFile的队列
    protected final MappedFileQueue mappedFileQueue;
    protected final DefaultMessageStore messageStore;

    //刷盘相关
    private final FlushCommitlogService flushCommitlogService;
    //direct bytebuffer相关
    private final FlushCommitlogService commitlogService;

    //写commitLog回调
    private final AppendMessageCallback appendMessageCallback;

    //<topic-queue, queueOffset>
    protected HashMap<String, Long> topicQueueOffsetTable = new HashMap<>(1024);
    protected volatile long confirmOffset = -1L;
    //加锁成功的时间戳
    private volatile long beginTimeInLock = 0;

    protected final PutMessageLock putMessageLock;

    public CommitLog(final DefaultMessageStore messageStore) {
        this.mappedFileQueue = new MappedFileQueue(messageStore.getMessageStoreConfig().getCommitlogStorePath(),
                messageStore.getMessageStoreConfig().getMappedFileSizeCommitlog(),
                messageStore.getAllocateMappedFileService());
        this.messageStore = messageStore;

        if (FlushDiskType.SYNC_FLUSH == messageStore.getMessageStoreConfig().getFlushDiskType()) {
            this.flushCommitlogService = new GroupCommitService();
        } else {
            this.flushCommitlogService = new FlushRealTimeService();
        }

        this.commitlogService = new CommitRealTimeService();

        //追加消息的回调
        this.appendMessageCallback = new DefaultAppendMessageCallback(messageStore.getMessageStoreConfig().getMaxMessageSize());

        //锁的具体实现
        this.putMessageLock = messageStore.getMessageStoreConfig().isUseReentrantLockWhenPutMessage() ? new PutMessageReentrantLock() : new PutMessageSpinLock();
    }

    public boolean load() {
        boolean result = this.mappedFileQueue.load();
        log.info("load commit log {}", result ? "ok" : "failed");
        return result;
    }

    public void start() {
        // 启动刷盘线程
        this.flushCommitlogService.start();

        if(this.messageStore.getMessageStoreConfig().isEnableTransientStorePool()){
            this.commitlogService.start();
        }
    }


    /**
     * 将消息追加到commitLog
     *
     * @param messageInner
     * @return
     */
    public CompletableFuture<PutMessageResult> asyncPutMessage(final MessageExtBrokerInner messageInner) {
        log.info("【CommitLog】开始asyncPutMessage, messageInner: {}", messageInner);
        //设置消息的存储时间
        messageInner.setStoreTimestamp(System.currentTimeMillis());
        //设置body CRC mock
        messageInner.setBodyCRC(UtilAll.crc32(messageInner.getBody()));

        //追加到commitLog
        AppendMessageResult result;

        //1、如果不是事务消息，或者事务消息的提交阶段
        String topic = messageInner.getTopic();
        int queueId = messageInner.getQueueId();

        int transType = MessageSysFlag.getTransactionValue(messageInner.getSysFlag());
        if (transType == MessageSysFlag.TRANSACTION_NOT_TYPE || transType == MessageSysFlag.TRANSACTION_COMMIT_TYPE) {
            //处理延迟消息的逻辑
            if (messageInner.getDelayTimeLevel() > 0) {
                //1、判断延迟消息的等级是否超过了最大级别限制

                //2、将消息的topic临时换成系统延迟消息的topic
                //根据延迟等级处理消息的队列

                //3、备份真实的topic、queueId

                //4、替换目前的topic、queueId
            }
        }

        //2、获取需要追加的mappedFile
        long elapsedTime = 0;
        MappedFile unlockMappedFile = null;
        //获取正在写消息的mappedFile
        MappedFile mappedFile = this.mappedFileQueue.getLastMappedFile();

        //3、加锁后追加消息，自旋锁或者可重入锁
        putMessageLock.lock();
        try {
            long beginLockTimestamp = this.messageStore.getSystemClock().now();
            this.beginTimeInLock = beginLockTimestamp;
            //重新更新消息的存储时间，保证消息全局有序
            messageInner.setStoreTimestamp(beginLockTimestamp);

            //校验mappedFile的合法性
            if (mappedFile == null || mappedFile.isFull()) {
                mappedFile = this.mappedFileQueue.getLastMappedFile(0);
            }

            //return
            if (mappedFile == null) {
                log.error("create mapped file error, topic:{}, client addr:{}", topic, messageInner.getBornHost());
                beginTimeInLock = 0;
                return CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.CREATE_MAPPED_FILE_FAILED, null));
            }
            result = mappedFile.appendMessage(messageInner, this.appendMessageCallback);

            //处理appendMessageResult
            assert result != null;
            switch (result.getAppendMessageStatus()) {
                case PUK_OK:
                    break;
                case END_OF_FILE:
                    unlockMappedFile = mappedFile;
                    //再次尝试创建新的mapped file，然后append message
                    mappedFile = this.mappedFileQueue.getLastMappedFile(0);
                    if (mappedFile == null) {
                        log.error("create mapped file error, topic:{}, client addr:{}", topic, messageInner.getBornHost());
                        beginTimeInLock = 0;
                        return CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.CREATE_MAPPED_FILE_FAILED, result));
                    }
                    result = mappedFile.appendMessage(messageInner, appendMessageCallback);
                    break;
                case PROPERTIES_SIZE_EXCEEDED:
                case MESSAGE_SIZE_EXCEEDED:
                    beginTimeInLock = 0;
                    return CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.MESSAGE_ILLEGAL, result));
                default:
                    beginTimeInLock = 0;
                    return CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.UNKNOWN_ERROR, result));
            }

            //返回PutMessageResult
            elapsedTime = this.messageStore.getSystemClock().now() - beginLockTimestamp;
            beginTimeInLock = 0;
        } finally {
            putMessageLock.unlock();
        }

        if (elapsedTime > 500) {
            log.warn("putMessage in lock cost time {}ms, bodyLength: {}, appendMessageResult: {}", elapsedTime, messageInner.getBody().length, result);
        }

        if (unlockMappedFile != null && this.messageStore.getMessageStoreConfig().isWarmupMappedFileEnable()) {
            this.messageStore.unlockMappedFile(unlockMappedFile);
        }

        //执行统计

        PutMessageResult putMessageResult = new PutMessageResult(PutMessageStatus.PUT_OK, result);


        //执行刷盘（写磁盘）
        CompletableFuture<PutMessageStatus> flushRequestFuture = submitFlushRequest(result, putMessageResult, messageInner);
        CompletableFuture<PutMessageStatus> flushReplicaRequestFuture = submitReplicaRequest(result, putMessageResult, messageInner);

        return flushRequestFuture.thenCombine(flushReplicaRequestFuture,
                (putMessageStatus, putMessageReplica) -> {
                    if (putMessageStatus != PutMessageStatus.PUT_OK) {
                        putMessageResult.setPutMessageStatus(PutMessageStatus.FLUSH_DISK_TIMEOUT);
                    }
                    if (putMessageReplica != PutMessageStatus.PUT_OK) {
                        putMessageResult.setPutMessageStatus(putMessageReplica);
                    }

                    return putMessageResult;
                });
    }

    //写磁盘
    private CompletableFuture<PutMessageStatus> submitFlushRequest(AppendMessageResult result, PutMessageResult putMessageResult, MessageExt messageInner) {
        //同步刷盘
        if (FlushDiskType.SYNC_FLUSH == messageStore.getMessageStoreConfig().getFlushDiskType()) {
            GroupCommitService groupCommitService = (GroupCommitService) this.flushCommitlogService;
            if (messageInner.isWaitStoreOk()) {
                GroupSubmitRequest submitRequest = new GroupSubmitRequest(result.getWroteOffset() + result.getWriteBytes(),
                        messageStore.getMessageStoreConfig().getSyncFlushTimeout());
                groupCommitService.putRequest(submitRequest);

                return submitRequest.future();
            } else {
                groupCommitService.wakeup();
                return CompletableFuture.completedFuture(PutMessageStatus.PUT_OK);
            }
        } else { //异步刷盘, 唤醒刷盘线程
            if(this.messageStore.getMessageStoreConfig().isEnableTransientStorePool()){
                flushCommitlogService.wakeup();
            }else{
                commitlogService.wakeup();
            }
            return CompletableFuture.completedFuture(PutMessageStatus.PUT_OK);
        }
    }

    private CompletableFuture<PutMessageStatus> submitReplicaRequest(AppendMessageResult result, PutMessageResult putMessageResult, MessageExtBrokerInner messageInner) {
        if (BrokerRole.SLAVE == this.messageStore.getMessageStoreConfig().getBrokerRole()) {
            //通过HaService来
        }
        return CompletableFuture.completedFuture(PutMessageStatus.PUT_OK);
    }

    public long getMaxOffset() {
        return this.mappedFileQueue.getMaxOffset();
    }

    public long getMinOffset() {
        MappedFile mappedFile = this.mappedFileQueue.getFirstMappedFile();
        if(mappedFile != null){
            if(mappedFile.isAvailable()){
                return mappedFile.getFileFromOffset();
            }else{
                return this.rollNextFile(mappedFile.getFileFromOffset());
            }
        }
        return -1;
    }

    public long getConfirmOffset() {
        return this.confirmOffset;
    }

    public SelectMappedBufferResult getData(long offset) {
        return getData(offset, offset == 0);
    }

    /**
     * doReput时调用
     * 先找到对应的mappedFile,然后根据mappedFile找到对应的MappedBuffer
     * @param offset 重放消息的起始偏移量
     * @param returnFirstOnNotFound
     * @return
     */
    public SelectMappedBufferResult getData(final long offset, final boolean returnFirstOnNotFound) {
        int mappedFileSize = this.messageStore.getMessageStoreConfig().getMappedFileSizeCommitlog();
        MappedFile mappedFile = this.mappedFileQueue.findMappedFileByOffset(offset, returnFirstOnNotFound);
        if (mappedFile != null) {
            int pos = (int) (offset % mappedFileSize);
            return mappedFile.selectMappedBuffer(pos);
        }
        return null;
    }

    public void recoverNormally(long maxPhyOffsetOfConsumeQueue) {
        boolean checkCRCOnRecover = this.messageStore.getMessageStoreConfig().isCheckCRCOnRecover();
        final List<MappedFile> mappedFiles = this.mappedFileQueue.getMappedFiles();
        if(!mappedFiles.isEmpty()){
            //began to recover from the last three files
            int index = mappedFiles.size() - 3;
            if(index < 0){
                index = 0;
            }
            MappedFile mappedFile = mappedFiles.get(index);
            ByteBuffer byteBuffer = mappedFile.sliceByteBuffer();
            long processOffset = mappedFile.getFileFromOffset();
            long mappedFileOffset = 0;
            while(true){
                DispatchRequest request = this.checkMessageAndReturnSize(byteBuffer, checkCRCOnRecover);
                int msgSize = request.getMsgSize();
                //normal data
                if(request.isSuccess() && msgSize > 0){
                    mappedFileOffset += msgSize;
                }else if(request.isSuccess() && msgSize == 0){
                    /*
                    came the end of file, switch to the next file since return 0 representatives
                    met last hole, this can not be included in truncate offset
                     */
                    index++;
                    if(index >= mappedFiles.size()){
                        //current branch not happen
                        log.info("recover the last 3 physical file over, last mapped file = {}", mappedFile.getFileName());
                        break;
                    }else{
                        mappedFile = mappedFiles.get(index);
                        byteBuffer = mappedFile.sliceByteBuffer();
                        processOffset = mappedFile.getFileFromOffset();
                        mappedFileOffset = 0;
                        log.info("recover next mapped file, {}", mappedFile.getFileName());
                    }
                }else if(!request.isSuccess()){  //intermediate file read error
                    log.info("recover physical file end, {}", mappedFile.getFileName());
                    break;
                }
            }

            processOffset += mappedFileOffset;

            this.mappedFileQueue.setFlushedWhere(processOffset);
            this.mappedFileQueue.setCommittedWhere(processOffset);
            this.mappedFileQueue.truncateDirtyFiles(processOffset);

            if(maxPhyOffsetOfConsumeQueue >= processOffset){
                log.warn("maxPhyOffsetOfConsumeQueue {}, greater than processOffset {},truncate dirty log files", maxPhyOffsetOfConsumeQueue, processOffset);
                this.messageStore.truncateDirtyLogicFiles(processOffset);
            }
        }else {
            //commit log are deleted
            log.info("the commit log file are deleted, delete logic files");
            this.mappedFileQueue.setFlushedWhere(0);
            this.mappedFileQueue.setCommittedWhere(0);
            this.messageStore.destroyLogicFiles();
        }
    }

    public long rollNextFile(long offset){
        int mappedFileSize = this.messageStore.getMessageStoreConfig().getMappedFileSizeCommitlog();
        return offset + mappedFileSize - offset % mappedFileSize;
    }

    public DispatchRequest checkMessageAndReturnSize(ByteBuffer byteBuffer, boolean checkCRC){
        return this.checkMessageAndReturnSize(byteBuffer, checkCRC, true);
    }

    /**
     * checkMessage and return message size
     * @param byteBuffer
     * @param checkCRC
     * @param readBody
     * @return = 0  come the end of the file
     *         > 0  normal messages
     *         = -1 check the message fail
     */
    public DispatchRequest checkMessageAndReturnSize(ByteBuffer byteBuffer, boolean checkCRC, boolean readBody) {
        try{
            //total size
            int totalSize = byteBuffer.getInt();

            int magicCode = byteBuffer.getInt();
            switch (magicCode){
                case MESSAGE_MAGIC_CODE:
                    break;
                case BLANK_MAGIC_CODE:
                    return new DispatchRequest(0, true);
                default:
                    log.warn("found illegal magic code 0x{}", Integer.toBinaryString(magicCode));
                    return new DispatchRequest(-1, false);
            }

            byte[] content = new byte[totalSize];
            int bodyCRC = byteBuffer.getInt();
            int queueId = byteBuffer.getInt();
            int flag = byteBuffer.getInt();
            long queueOffset = byteBuffer.getLong();
            long physicOffset = byteBuffer.getLong();
            int sysFlag = byteBuffer.getInt();
            long bornTimestamp = byteBuffer.getLong();

            ByteBuffer byteBuffer1;
            if((sysFlag & MessageSysFlag.FLAG_BORN_HOST_V6) == 0){
                byteBuffer1 = byteBuffer.get(content, 0, 4 + 4);
            }else{
                byteBuffer1 = byteBuffer.get(content, 0, 16 + 4);
            }

            long storeTimestamp = byteBuffer.getLong();
            ByteBuffer byteBuffer2;
            if((sysFlag & MessageSysFlag.FLAG_STORE_HOST_V6) == 0){
                byteBuffer2 = byteBuffer.get(content, 0, 4 + 4);
            }else {
                byteBuffer2 = byteBuffer.get(content, 0, 16 + 4);
            }

            int reconsumeTimes = byteBuffer.getInt();
            long prepareTransactionOffset = byteBuffer.getLong();

            int bodyLen = byteBuffer.getInt();
            if(bodyLen > 0){
                if(readBody){
                    byteBuffer.get(content, 0, bodyLen);
                    if(checkCRC){
                        //todo checkCRC
                        //return new DispatchRequest(-1, false);
                        int crc = UtilAll.crc32(content, 0, bodyLen);
                        if(crc != bodyCRC){
                            return new DispatchRequest(-1, false);
                        }
                    }
                }else {
                    byteBuffer.position(byteBuffer.position() + bodyLen);
                }
            }

            byte topicLen = byteBuffer.get();
            byteBuffer.get(content, 0, topicLen);
            String topic = new String(content, 0, topicLen, StandardCharsets.UTF_8);

            long tagsCode = 0;
            String keys = "";
            String uniqKey = null;

            short propertiesLen = byteBuffer.getShort();
            Map<String, String> propertiesMap = null;
            if(propertiesLen > 0){
                byteBuffer.get(content, 0, propertiesLen);
                String properties = new String(content, 0, propertiesLen, StandardCharsets.UTF_8);
                propertiesMap = MessageDecoder.string2Properties(properties);

                keys = propertiesMap.get(MessageConst.PROPERTY_KEYS);
                uniqKey = propertiesMap.get(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX);
                String tags = propertiesMap.get(MessageConst.PROPERTY_TAGS);

                if(tags != null && tags.length() > 0){
                    tagsCode = MessageExtBrokerInner.tagsString2tagsCode(MessageExt.parseTopicFilterType(sysFlag), tags);
                }

                //处理定时消息
                {
                    String t = propertiesMap.get(MessageConst.PROPERTY_DELAY_LEVEL);
                    if(TopicValidator.RMQ_SYS_SCHEDULE_TOPIC.equals(topic) && t != null){
                        int delayLevel = Integer.parseInt(t);
                        delayLevel = Math.min(delayLevel, messageStore.getScheduleMessageService().getMaxDelayLevel());

                        if(delayLevel > 0){
                            //计算延迟时间
                            tagsCode = messageStore.getScheduleMessageService().computeDeliverTimestamp(delayLevel, storeTimestamp);
                        }
                    }
                }
            }
            //计算消息的长度
            int readLength = calculateMessageLength(sysFlag, bodyLen, topicLen, propertiesLen);
            if(totalSize != readLength){
                // 读出来的消息长度不相等
                return new DispatchRequest(totalSize, false);
            }

            return new DispatchRequest(
                topic, queueId, physicOffset,totalSize, tagsCode, storeTimestamp, queueOffset, keys, uniqKey, sysFlag, prepareTransactionOffset, propertiesMap
            );
        }catch (Exception e){

        }
        return new DispatchRequest(-1, false);
    }

    protected static int calculateMessageLength(int sysFlag, int propertiesLength, int topicLength, int bodyLength) {
        int bornHostLength = (sysFlag & MessageSysFlag.FLAG_BORN_HOST_V6) == 0 ? (4 + 4) : (4 + 16);
        int storeHostLength = (sysFlag & MessageSysFlag.FLAG_STORE_HOST_V6) == 0 ? (4 + 4) : (4 + 16);
        int totalSize = MessageStruct.MSG_TOTAL_SIZE;
        int magicCode = MessageStruct.MSG_MAGIC_CODE;
        int bodyCRC = MessageStruct.MSG_BODY_CRC;
        int queueId = MessageStruct.MSG_QUEUE_ID;
        int flag = MessageStruct.MSG_FLAG;
        int queueOffset = MessageStruct.MSG_QUEUE_OFFSET;
        int physicalOffset = MessageStruct.MSG_PHYSICAL_OFFSET;
        int sysFlagLength = MessageStruct.MSG_SYS_FLAG;
        int bornTimestamp = MessageStruct.MSG_BORN_TIMESTAMP;
        int storeTimestamp = MessageStruct.MSG_STORE_TIMESTAMP;
        int reconsumeTimes = MessageStruct.MSG_RECONSUME_TIMES;
        int prepareTransactionOffset = MessageStruct.MSG_PREPARE_TRANS_OFFSET;
        int bodyTotalLength = 4 + Math.max(bodyLength, 0);
        int topicTopicLength = 1 + topicLength;
        int propertiesTotalLength = 2 + Math.max(propertiesLength, 0);
        return totalSize + magicCode + bodyCRC + queueId + flag + queueOffset + physicalOffset + sysFlagLength +
                bornTimestamp + bornHostLength + storeTimestamp + storeHostLength + reconsumeTimes + prepareTransactionOffset +
                bodyTotalLength + topicTopicLength + propertiesTotalLength;
    }

    /**
     * 真正的逻辑是找到对应的mappedFile，然后通过mappedFile取对应的字节
     * @param offset
     * @param size
     * @return
     */
    public SelectMappedBufferResult getMessage(final long offset,final int size) {
        int mappedFileSize = this.messageStore.getMessageStoreConfig().getMappedFileSizeCommitlog();
        MappedFile mappedFile = this.mappedFileQueue.findMappedFileByOffset(offset, offset == 0);
        if(mappedFile != null){
            int pos = (int) (offset % mappedFileSize);
            return mappedFile.selectMappedBuffer(pos, size);
        }

        return null;
    }

    public void recoverAbnormally(long maxPhyOffsetOfConsumeQueue) {

    }

    public void setTopicQueueTable(HashMap<String, Long> table) {
        this.topicQueueOffsetTable = table;
    }


    public static class GroupSubmitRequest {
        private long nextOffset;
        private long syncFlushTimeout;
        private final CompletableFuture<PutMessageStatus> flushOkFuture = new CompletableFuture<>();

        public GroupSubmitRequest(long nextOffset, long syncFlushTimeout) {
            this.nextOffset = nextOffset;
            this.syncFlushTimeout = syncFlushTimeout;
        }

        public CompletableFuture<PutMessageStatus> future() {
            return this.flushOkFuture;
        }

        public void wakeupCustomer(final PutMessageStatus putMessageStatus) {
            this.flushOkFuture.complete(putMessageStatus);
        }

        public long getNextOffset() {
            return nextOffset;
        }

        public void setNextOffset(long nextOffset) {
            this.nextOffset = nextOffset;
        }

        public long getSyncFlushTimeout() {
            return syncFlushTimeout;
        }

        public void setSyncFlushTimeout(long syncFlushTimeout) {
            this.syncFlushTimeout = syncFlushTimeout;
        }
    }

    /**
     * 刷盘服务抽象类
     */
    abstract static class FlushCommitlogService extends ServiceThread {
        protected static final int RETRY_TIMES_OVER = 10;
    }

    /**
     * 异步刷盘线程
     */
    static class FlushRealTimeService extends FlushCommitlogService {

        @Override
        public String getServiceName() {
            return FlushRealTimeService.class.getSimpleName();
        }

        @Override
        public void run() {

        }
    }

    /**
     * 开启transientPool刷盘线程
     */
    static class CommitRealTimeService extends FlushCommitlogService {
        @Override
        public String getServiceName() {
            return CommitRealTimeService.class.getSimpleName();
        }

        @Override
        public void run() {

        }
    }

    /**
     * 同步刷盘线程
     */
    class GroupCommitService extends FlushCommitlogService {

        private volatile List<GroupSubmitRequest> writeRequests = new ArrayList<>();
        private volatile List<GroupSubmitRequest> readRequests = new ArrayList<>();

        @Override
        public String getServiceName() {
            return GroupCommitService.class.getSimpleName();
        }

        @Override
        public void run() {
            log.info(getServiceName() + " started");
            while (!this.isStopped()) {
                try {
                    /*
                     * 线程启动后先阻塞在这里10ms
                     * 10ms轮询一次
                     * 当有刷盘请求提交时，会唤醒刷盘线程，不会阻塞 wakeup()
                     */
                    this.waitForRunning(10);
                    this.doCommit();
                } catch (Exception e) {
                    log.error(getServiceName() + " has an exception", e);
                }
            }

            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                log.error("interrupted", e);
            }

            synchronized (this) {
                this.swapRequests();
            }

            this.doCommit();
            log.info(getServiceName() + "stopped");
        }

        public synchronized void putRequest(final GroupSubmitRequest request) {
            log.info("【GroupCommitService】提交刷盘请求");
            synchronized (writeRequests) {
                this.writeRequests.add(request);
            }
            //hasNotified  false  =>  true
            this.wakeup();
        }

        /**
         * 交换两个存放请求的容器
         */
        public void swapRequests() {
            List<GroupSubmitRequest> tmp = writeRequests;
            this.writeRequests = this.readRequests;
            this.readRequests = tmp;
        }

        private void doCommit() {
            synchronized (this.readRequests) {
                // log.info("【Commitlog】刷盘请求数：{}", this.readRequests.size());
                if (this.readRequests.isEmpty()) {
                    //因为一些消息被设置为非同步刷盘， 所以会走到这个逻辑
                    CommitLog.this.mappedFileQueue.flush(0);
                } else {
                    //因为有可能消息在下一个mapped file，所以最大刷盘两次
                    for (GroupSubmitRequest readRequest : this.readRequests) {

                        boolean flushOk = false;
                        for (int i = 0; i < 2 && !flushOk; i++) {
                            //最大刷盘位置大于等于最大偏移量，表示刷盘成功
                            flushOk = CommitLog.this.mappedFileQueue.getFlushedWhere() >= readRequest.getNextOffset();
                            if (!flushOk) {
                                CommitLog.this.mappedFileQueue.flush(0);
                            }
                        }
                        //将刷盘结果填充到request的future中
                        readRequest.wakeupCustomer(flushOk ? PutMessageStatus.PUT_OK : PutMessageStatus.FLUSH_DISK_TIMEOUT);
                    }

                    long storeTimestamp = CommitLog.this.mappedFileQueue.getStoreTimestamp();
                    if (storeTimestamp >= 0) {
                        CommitLog.this.messageStore.getStoreCheckpoint().setPhysicalMsgTimestamp(storeTimestamp);
                    }

                    this.readRequests.clear();
                }
            }
        }

        @Override
        public void onWaitEnd() {
            this.swapRequests();
        }

        /**
         * 线程停止
         * @return
         */
        @Override
        public long getJoinTime() {
            return 5 * 60 * 1000;
        }

        public List<GroupSubmitRequest> getWriteRequests() {
            return writeRequests;
        }

        public void setWriteRequests(List<GroupSubmitRequest> writeRequests) {
            this.writeRequests = writeRequests;
        }

        public List<GroupSubmitRequest> getReadRequests() {
            return readRequests;
        }

        public void setReadRequests(List<GroupSubmitRequest> readRequests) {
            this.readRequests = readRequests;
        }
    }

    class DefaultAppendMessageCallback implements AppendMessageCallback {
        private static final int END_FILE_MIN_BLANK_LENGTH = 4 + 4;
        private final ByteBuffer msgIdMemory;
        private final ByteBuffer msgIdV6Memory;

        private final ByteBuffer msgStoreItemMemory;
        private final int maxMessageSize;

        private final StringBuilder keyBuilder = new StringBuilder();
        private final StringBuilder msgIdBuilder = new StringBuilder();

        public DefaultAppendMessageCallback(int maxMessageSize) {
            this.msgIdMemory = ByteBuffer.allocate(4 + 4 + 8);
            this.msgIdV6Memory = ByteBuffer.allocate(16 + 4 + 8);
            this.msgStoreItemMemory = ByteBuffer.allocate(maxMessageSize + END_FILE_MIN_BLANK_LENGTH);
            this.maxMessageSize = maxMessageSize;
        }

        @Override
        public AppendMessageResult doAppend(long fileFromOffset, ByteBuffer byteBuffer, int maxBlank, MessageExtBrokerInner messageInner) {

            long wroteOffset = fileFromOffset + byteBuffer.position();
            int sysFlag = messageInner.getSysFlag();
            int bornHostLen = (sysFlag & MessageSysFlag.FLAG_BORN_HOST_V6) == 0 ? 4 + 4 : 16 + 4;
            int storeHostLen = (sysFlag & MessageSysFlag.FLAG_STORE_HOST_V6) == 0 ? 4 + 4 : 16 + 4;
            ByteBuffer bornHostBuffer = ByteBuffer.allocate(bornHostLen);
            ByteBuffer storeHostBuffer = ByteBuffer.allocate(storeHostLen);

            this.resetByteBuffer(storeHostBuffer, storeHostLen);
            String msgId;
            if ((sysFlag & MessageSysFlag.FLAG_STORE_HOST_V6) == 0) {
                msgId = MessageDecoder.createdMsgId(this.msgIdMemory, messageInner.getStoreHostBytes(storeHostBuffer), wroteOffset);
            } else {
                msgId = MessageDecoder.createdMsgId(this.msgIdV6Memory, messageInner.getStoreHostBytes(storeHostBuffer), wroteOffset);
            }

            //记录consumerQueue信息
            keyBuilder.setLength(0);
            keyBuilder.append(messageInner.getTopic());
            keyBuilder.append("-");
            keyBuilder.append(messageInner.getQueueId());
            String key = keyBuilder.toString();
            long queueOffset = CommitLog.this.topicQueueOffsetTable.computeIfAbsent(key, k -> 0L);

            //处理事务消息
            final int transType = MessageSysFlag.getTransactionValue(messageInner.getSysFlag());
            switch (transType) {
                //事务消息半消息、回滚消息不会进入consumeQueue
                case MessageSysFlag.TRANSACTION_PREPARE_TYPE:
                case MessageSysFlag.TRANSACTION_ROLLBACK_TYPE:
                    queueOffset = 0L;
                    break;
                case MessageSysFlag.TRANSACTION_NOT_TYPE:
                case MessageSysFlag.TRANSACTION_COMMIT_TYPE:
                default:
                    break;
            }

            //properties
            final byte[] propertiesData = messageInner.getPropertiesString() == null ? null : messageInner.getPropertiesString().getBytes(StandardCharsets.UTF_8);
            final int propertiesLength = propertiesData == null ? 0 : propertiesData.length;

            if (propertiesLength > Short.MAX_VALUE) {
                log.warn("put message properties data too long, length: {}", propertiesLength);
                return new AppendMessageResult(AppendMessageStatus.PROPERTIES_SIZE_EXCEEDED);
            }

            //topic length 和 body length
            final byte[] topicData = messageInner.getTopic().getBytes(StandardCharsets.UTF_8);
            final int topicLength = topicData.length;

            final int bodyLength = messageInner.getBody() == null ? 0 : messageInner.getBody().length;
            //calculate the total length of message
            final int messageLength = calculateMessageLength(messageInner.getSysFlag(), propertiesLength, topicLength, bodyLength);

            if (messageLength > this.maxMessageSize) {
                log.warn("put message message length too long, total length: {}", messageLength);
                return new AppendMessageResult(AppendMessageStatus.MESSAGE_SIZE_EXCEEDED);
            }

            if (messageLength + END_FILE_MIN_BLANK_LENGTH > maxBlank) {
                this.resetByteBuffer(this.msgStoreItemMemory, maxBlank);
                //填充这段不足的mapped file的剩余空间
                this.msgStoreItemMemory.putInt(maxBlank);

                this.msgStoreItemMemory.putInt(CommitLog.BLANK_MAGIC_CODE);

                //随便填充值 any value
                long beginTimeMillis = CommitLog.this.messageStore.getSystemClock().now();
                //特别将length 设置为maxBlank
                byteBuffer.put(this.msgStoreItemMemory.array(), 0, maxBlank);

                log.warn("put message does not have enough space to append message, messageLength: {}, maxBlank: {}", messageLength, maxBlank);
                return new AppendMessageResult(AppendMessageStatus.END_OF_FILE, wroteOffset, maxBlank, msgId, messageInner.getStoreTimestamp(),
                        queueOffset, System.currentTimeMillis() - beginTimeMillis);
            }

            //初始化消息存储byteBuffer
            this.resetByteBuffer(this.msgStoreItemMemory, messageLength);
            //1、totalSize
            this.msgStoreItemMemory.putInt(messageLength);
            //2、message_magic_code
            this.msgStoreItemMemory.putInt(CommitLog.MESSAGE_MAGIC_CODE);
            //3、body CRC
            this.msgStoreItemMemory.putInt(messageInner.getBodyCRC());
            //4、queueId
            this.msgStoreItemMemory.putInt(messageInner.getQueueId());
            //5、flag
            this.msgStoreItemMemory.putInt(messageInner.getFlag());
            //6、queueOffset
            this.msgStoreItemMemory.putLong(queueOffset);
            //7、physical offset -- wroteOffset
            this.msgStoreItemMemory.putLong(byteBuffer.position() + fileFromOffset);
            //8、sysFlag
            this.msgStoreItemMemory.putInt(messageInner.getSysFlag());
            //9、bornTimestamp
            this.msgStoreItemMemory.putLong(messageInner.getBornTimestamp());
            //10、bornHost
            this.resetByteBuffer(bornHostBuffer, bornHostLen);
            this.msgStoreItemMemory.put(messageInner.getBornHostBytes(bornHostBuffer));
            //11、storeTimestamp
            this.msgStoreItemMemory.putLong(messageInner.getStoreTimestamp());
            //12、storeHost
            this.resetByteBuffer(storeHostBuffer, storeHostLen);
            this.msgStoreItemMemory.put(messageInner.getStoreHostBytes(storeHostBuffer));
            //13、reconsumeTimes
            this.msgStoreItemMemory.putInt(messageInner.getReconsumeTimes());
            //14、prepareTransactionOffset
            this.msgStoreItemMemory.putLong(messageInner.getPreparedTransactionOffset());
            //15、body
            this.msgStoreItemMemory.putInt(bodyLength);
            if (bodyLength > 0) {
                this.msgStoreItemMemory.put(messageInner.getBody());
            }
            //16、topic
            this.msgStoreItemMemory.put((byte) topicLength);
            this.msgStoreItemMemory.put(topicData);
            //17、properties
            this.msgStoreItemMemory.putShort((short) propertiesLength);
            if (propertiesLength > 0) {
                this.msgStoreItemMemory.put(propertiesData);
            }

            long beginTimeMillis = CommitLog.this.messageStore.getSystemClock().now();
            byteBuffer.put(this.msgStoreItemMemory.array(), 0, messageLength);

            AppendMessageResult result = new AppendMessageResult(AppendMessageStatus.PUK_OK, wroteOffset, messageLength, msgId, messageInner.getStoreTimestamp(), queueOffset, System.currentTimeMillis() - beginTimeMillis);

            switch (transType) {
                case MessageSysFlag.TRANSACTION_NOT_TYPE:
                case MessageSysFlag.TRANSACTION_COMMIT_TYPE:
                    CommitLog.this.topicQueueOffsetTable.put(key, ++queueOffset);
                    break;
                default:
                    break;
            }

            return result;
        }

        @Override
        public AppendMessageResult doAppend(long fileFromOffset, ByteBuffer byteBuffer, int maxBlank, MessageExtBatch messageExtBatch) {
            return null;
        }

        private void resetByteBuffer(final ByteBuffer byteBuffer, final int limit) {
            byteBuffer.flip();
            byteBuffer.limit(limit);
        }

    }
}
