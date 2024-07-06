package edu.hubu.store;

import edu.hubu.store.config.BrokerRole;
import edu.hubu.store.config.StorePathConfigHelper;
import edu.hubu.store.consumeQueue.DispatchRequest;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.nio.ByteBuffer;

/**
 * @author: sugar
 * @date: 2023/7/15
 * @description:
 */
@Slf4j
public class ConsumeQueue {

    //offset + length + tagHash  8 + 4 + 8
    public static final int CONSUME_QUEUE_UNIT_SIZE = 20;

    private long maxPhysicOffset = -1;

    private final DefaultMessageStore defaultMessageStore;
    private final MappedFileQueue mappedFileQueue;

    private final String topic;
    private final int queueId;
    private final String storePath;
    private final ByteBuffer byteBufferIndex;

    private final int mappedFileSize;
    private volatile long minLogicOffset = 0;

    private ConsumeQueueExt consumeQueueExt = null;

    public ConsumeQueue(String topic, int queueId,String storePath, int mappedFileSize, DefaultMessageStore defaultMessageStore) {
        this.defaultMessageStore = defaultMessageStore;
        this.topic = topic;
        this.queueId = queueId;
        this.storePath = storePath;
        this.mappedFileSize = mappedFileSize;

        this.byteBufferIndex = ByteBuffer.allocate(CONSUME_QUEUE_UNIT_SIZE);
        String queuePath = storePath + File.separator + topic + File.separator + queueId;
        this.mappedFileQueue = new MappedFileQueue(queuePath, mappedFileSize, null);

        if(defaultMessageStore.getMessageStoreConfig().isEnableConsumeQueueExt()){
            this.consumeQueueExt = new ConsumeQueueExt(
                    topic,
                    queueId,
                    StorePathConfigHelper.getStorePathConsumeQueueExt(defaultMessageStore.getMessageStoreConfig().getStorePathRootDir()),
                    this.defaultMessageStore.getMessageStoreConfig().getMappedFileSizeConsumeQueueExt(),
                    this.defaultMessageStore.getMessageStoreConfig().getBitmapLengthConsumeQueueExt()
                    );
        }
    }

    public boolean load() {
        boolean result = this.mappedFileQueue.load();
        log.info("load consume queue " + this.topic + "-" + this.queueId + " " + (result ? "OK" : "Failed"));
        if(isExtReadEnable()){
            result  = result && this.consumeQueueExt.load();
        }

        return result;
    }

    public boolean flush(int flushConsumeQueuePages) {
        boolean result = this.mappedFileQueue.flush(flushConsumeQueuePages);

        if(isExtReadEnable()){
            result = result && this.consumeQueueExt.flush(flushConsumeQueuePages);
        }

        return result;
    }

    public long getMinOffsetInQueue() {
        return this.minLogicOffset / CONSUME_QUEUE_UNIT_SIZE;
    }

    public long getMaxOffsetInQueue() {
        return this.mappedFileQueue.getMaxOffset() / CONSUME_QUEUE_UNIT_SIZE;
    }

    /**
     * 构建consumeQueue
     * @param request
     */
    public void putMessagePositionInfoWrapper(DispatchRequest request) {
        final int maxRetries = 30;
        boolean canWrite = this.defaultMessageStore.getRunningFlags().isCQWritable();

        for(int i = 0; i < maxRetries && canWrite; i++){
            long tagsCode = request.getTagsCode();
            if(isExtWriteEnable()){
                ConsumeQueueExt.CqUnitExt cqUnitExt = new ConsumeQueueExt.CqUnitExt();
                cqUnitExt.setFilterBitmap(request.getBitMap());
                cqUnitExt.setMsgStoreTime(request.getStoreTimestamp());
                cqUnitExt.setTagsCode(request.getTagsCode());

                long extAddr = this.consumeQueueExt.put(cqUnitExt);
                if(isExtAddr(extAddr)){
                    tagsCode = extAddr;
                }else{
                    log.warn("save consume queue extend fail ");
                }
            }

            boolean result = this.putMessagePositionInfo(request.getCommitLogOffset(), request.getMsgSize(), tagsCode, request.getConsumeQueueOffset());

            if(result){
                if(this.defaultMessageStore.getMessageStoreConfig().getBrokerRole() == BrokerRole.SLAVE ||
                    this.defaultMessageStore.getMessageStoreConfig().isEnableDLedgerCommitlog()) {
                    this.defaultMessageStore.getStoreCheckpoint().setPhysicalMsgTimestamp(request.getStoreTimestamp());
                }
                    this.defaultMessageStore.getStoreCheckpoint().setLogicMsgTimestamp(request.getStoreTimestamp());
                return;
            }else {
                log.warn("[BUG]put commit log position info to topic = {}, queueId={} failed, commit log offset = {}, retry times = {}", topic, queueId, request.getCommitLogOffset(), i);

                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    log.warn("put message position info exception", e);
                }

            }
        }

        log.error("[BUG]consume queue can not write, {}, {}", this.topic, this.queueId);
        this.defaultMessageStore.getRunningFlags().makeLogicQueueError();
    }

    private boolean putMessagePositionInfo(long offset, int size, long tagsCode, long cqOffset) {

        if(offset + size <= maxPhysicOffset){
            log.info("maybe try to build consume queue repeatedly, maxPhysicOffset = {}, offset={}", maxPhysicOffset, offset);
            return true;
        }

        this.byteBufferIndex.flip();
        this.byteBufferIndex.limit(CONSUME_QUEUE_UNIT_SIZE);
        this.byteBufferIndex.putLong(offset);
        this.byteBufferIndex.putInt(size);
        this.byteBufferIndex.putLong(tagsCode);

        final long expectedOffset = cqOffset * CONSUME_QUEUE_UNIT_SIZE;
        MappedFile mappedFile = this.mappedFileQueue.getLastMappedFile(expectedOffset);
        if(mappedFile != null){
            if(mappedFile.isFirstCreateInQueue() && cqOffset != 0 && mappedFile.getWrotePosition() == 0){
                this.minLogicOffset = expectedOffset;

                this.mappedFileQueue.setFlushedWhere(expectedOffset);
                this.mappedFileQueue.setCommittedWhere(expectedOffset);

                this.fillPreBlank(mappedFile, expectedOffset);
                log.info("fill blank space {}, expectOffset {}, wrotePosition {} ", mappedFile.getFileName(),
                        expectedOffset, mappedFile.getWrotePosition());
            }

            if(cqOffset != 0){
                long currentLogicOffset = mappedFile.getFileFromOffset() +  mappedFile.getWrotePosition();
                 //重复构建consume queue
                if(expectedOffset < currentLogicOffset){
                    log.info("build consume queue repeatedly, currentOffset: {}, expectedOffset:{}, topic:{}, qid:{}, diff:{}", currentLogicOffset,
                            expectedOffset, topic, queueId, expectedOffset - currentLogicOffset);
                    return true;
                }

                if(expectedOffset != currentLogicOffset){
                    log.warn("[BUG] logic queue order maybe wrong, expectedLogicOffset:{}, currentOffset:{}, topic:{}, qid:{}, diff:{}",
                            expectedOffset, currentLogicOffset, topic, queueId, expectedOffset - currentLogicOffset);
                }

            }

            this.maxPhysicOffset = offset + size;
            return mappedFile.appendMessage(this.byteBufferIndex.array());
        }

        return false;
    }

    public SelectMappedBufferResult getIndexBuffer(long startIndex) {
        int mappedFileSize = this.mappedFileSize;
        long offset = startIndex * CONSUME_QUEUE_UNIT_SIZE;
        if(offset >= this.getMinLogicOffset()){
            MappedFile mappedFile = this.mappedFileQueue.findMappedFileByOffset(offset);
            if(mappedFile != null){
               return mappedFile.selectMappedBuffer((int) (offset % mappedFileSize));
            }
        }

        return null;
    }

    public long rollNextFile(final long index) {
        int mappedFileSize = this.mappedFileSize;
        int totalUnits = mappedFileSize / CONSUME_QUEUE_UNIT_SIZE;
        return index + totalUnits - index % totalUnits;
    }

    public boolean getExt(long tagsCode, ConsumeQueueExt.CqUnitExt cqUnitExt) {
        return false;
    }

    public void fillPreBlank(final MappedFile mappedFile, final long offset) {
        ByteBuffer buffer = ByteBuffer.allocate(CONSUME_QUEUE_UNIT_SIZE);
        buffer.putLong(0L);
        buffer.putInt(Integer.MAX_VALUE);  //size
        buffer.putLong(0L);

        int until = (int) (offset / this.mappedFileQueue.getMappedFileSize());
        for (int i = 0; i < until; i += CONSUME_QUEUE_UNIT_SIZE) {
            mappedFile.appendMessage(buffer.array());
        }
    }

    public boolean isExtAddr(long tagsCode){
        return ConsumeQueueExt.isExtAddr(tagsCode);
    }

    protected boolean isExtReadEnable(){
        return this.consumeQueueExt != null;
    }

    protected boolean isExtWriteEnable(){
        return this.consumeQueueExt != null &&
                this.defaultMessageStore.getMessageStoreConfig().isEnableConsumeQueueExt();
    }


    public long getMaxPhysicOffset() {
        return maxPhysicOffset;
    }

    public void setMaxPhysicOffset(long maxPhysicOffset) {
        this.maxPhysicOffset = maxPhysicOffset;
    }

    public DefaultMessageStore getDefaultMessageStore() {
        return defaultMessageStore;
    }

    public MappedFileQueue getMappedFileQueue() {
        return mappedFileQueue;
    }

    public String getTopic() {
        return topic;
    }

    public int getQueueId() {
        return queueId;
    }

    public String getStorePath() {
        return storePath;
    }

    public ByteBuffer getByteBufferIndex() {
        return byteBufferIndex;
    }

    public int getMappedFileSize() {
        return mappedFileSize;
    }

    public long getMinLogicOffset() {
        return minLogicOffset;
    }

}
