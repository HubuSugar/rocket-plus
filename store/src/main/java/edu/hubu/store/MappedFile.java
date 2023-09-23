package edu.hubu.store;

import com.sun.jna.NativeLong;
import com.sun.jna.Pointer;
import edu.hubu.common.message.MessageExt;
import edu.hubu.common.message.MessageExtBatch;
import edu.hubu.common.message.MessageExtBrokerInner;
import edu.hubu.store.config.FlushDiskType;
import edu.hubu.store.util.LibC;
import lombok.extern.slf4j.Slf4j;
import sun.nio.ch.DirectBuffer;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author: sugar
 * @date: 2023/7/16
 * @description:
 */
@Slf4j
public class MappedFile extends ReferenceResource{
    //pagecache的大小
    public static final int OS_PAGE_SIZE = 1024 * 4;
    //总共映射的虚拟空间大小
    private static final AtomicLong TOTAL_MAPPED_VIRTUAL_MEMORY_SIZE = new AtomicLong(0);
    //总共映射的mappedFiles文件数量
    private static final AtomicInteger TOTAL_MAPPED_FILES = new AtomicInteger(0);
    //文件写的位置 ？？
    protected final AtomicInteger wrotePosition = new AtomicInteger(0);
    //文件提交的位置 ？？
    protected final AtomicInteger committedPosition = new AtomicInteger(0);
    //文件刷盘位置
    private final AtomicInteger flushedPosition = new AtomicInteger(0);
    //文件大小
    protected int fileSize;
    protected FileChannel fileChannel;

    private String fileName;
    //当前文件的起始偏移量（通过文件名获取）
    private long fileFromOffset;
    private File file;

    private TransientStorePool transientStorePool;
    private ByteBuffer writeBuffer;
    private MappedByteBuffer mappedByteBuffer;

    private volatile long storeTimestamp = 0;
    private boolean firstCreateInQueue = false;

    public MappedFile() {
    }

    public MappedFile(final String filePath, final int mappedFileSize) throws IOException {
        init(filePath, mappedFileSize);
    }

    public MappedFile(final String filePath, final int mappedFileSize, final TransientStorePool pool) throws IOException{
        init(filePath, mappedFileSize, pool);
    }

    @Override
    public boolean cleanup(int refCount) {
        if(this.isAvailable()){
            return false;
        }
        if(this.isCleanupOver()){
            return true;
        }

        clean(this.mappedByteBuffer);

        TOTAL_MAPPED_VIRTUAL_MEMORY_SIZE.addAndGet(fileSize * (-1));
        TOTAL_MAPPED_FILES.decrementAndGet();
        log.warn("");
        return true;
    }

    public void clean(final ByteBuffer byteBuffer){
        if(byteBuffer == null || !byteBuffer.isDirect() || byteBuffer.capacity() == 0) return;
        invoke(invoke(viewed(byteBuffer), "cleaner"), "clean");
    }

    public static ByteBuffer viewed(final ByteBuffer byteBuffer){
        String methodName = "viewedBuffer";
        Method[] methods = byteBuffer.getClass().getDeclaredMethods();
        for (Method method : methods) {
            if(method.getName().equals("attachment")){
                methodName = "attachment";
                break;
            }
        }

       ByteBuffer viewedBuffer = (ByteBuffer) invoke(byteBuffer, methodName);
        if(viewedBuffer == null)return byteBuffer;
        else {
            return viewed(viewedBuffer);
        }
    }

    public static Object invoke(final Object obj, final String methodName,final Class<?>... args){
        return AccessController.doPrivileged(new PrivilegedAction<Object>() {
            @Override
            public Object run() {
                try {
                    Method method = method(obj, methodName, args);
                    method.setAccessible(true);
                    return method.invoke(obj);
                } catch (Exception e) {
                    throw new IllegalStateException(e);
                }
            }
        });
    }

    public static Method method(final Object obj, String method, final Class<?>[] args) throws NoSuchMethodException {

        try {
            return obj.getClass().getMethod(method, args);
        } catch (NoSuchMethodException e) {
            return obj.getClass().getDeclaredMethod(method, args);
        }
    }

    /**
     * 使用transientPool时，需要先初始化writeBuffer
     * @param fileName
     * @param fileSize
     * @param transientStorePool
     * @throws IOException
     */
    public void init(final String fileName, final int fileSize, final TransientStorePool transientStorePool) throws IOException {
        init(fileName, fileSize);
        this.writeBuffer = transientStorePool.borrowBuffer();
        this.transientStorePool = transientStorePool;
    }

    public void init(final String fileName, final int fileSize) throws IOException{
        this.fileName = fileName;
        this.fileSize = fileSize;
        this.file = new File(fileName);
        this.fileFromOffset = Long.parseLong(this.file.getName());

        boolean ok = false;

        ensureDirOk(this.file.getParent());
        try{
            this.fileChannel = new RandomAccessFile(this.file, "rw").getChannel();
            this.mappedByteBuffer = this.fileChannel.map(FileChannel.MapMode.READ_WRITE, 0 , fileSize);
            TOTAL_MAPPED_VIRTUAL_MEMORY_SIZE.addAndGet(fileSize);
            TOTAL_MAPPED_FILES.incrementAndGet();
            ok = true;
        }catch (FileNotFoundException e){
            log.error("fail to create mapped file", e);
            throw e;
        } catch (IOException e) {
            log.error("fail to map file", e);
            throw e;
        }finally {
            if(!ok && this.fileChannel != null){
                this.fileChannel.close();
            }
        }
    }

    /**
     *
     * @param flushLeastPages
     * @return 当前的刷盘位置
     */
    public int flush(int flushLeastPages){
        if(isAbleToFlush(flushLeastPages)){
            if(this.isHold()){
                int value = getReadPosition();
                try {
                    //we only append data to fileChannel or mappedByteBuffer, never both
                    if(this.writeBuffer != null || this.fileChannel.position() != 0){
                        this.fileChannel.force(false);
                    }else{
                        this.mappedByteBuffer.force();
                    }
                } catch (Throwable e) {
                    log.error("error occurred when force flush data to disk", e);
                }

                this.flushedPosition.set(value);
                this.release();
            }else{
                log.warn("in flush hold failed, flush offset = {}", this.flushedPosition.get());
            }
        }
        return this.getFlushedPosition();
    }

    public boolean isAbleToFlush(int flushLeastPages){
        int flush = this.flushedPosition.get();
        int write = this.getReadPosition();
        if(isFull()){
            return false;
        }

        if(flushLeastPages > 0){
            return ((write / OS_PAGE_SIZE) - (flush / OS_PAGE_SIZE))  >= flushLeastPages;
        }


        return write > flush;
    }

    /**
     * 获取需要执行doReput的一段消息
     * <p>|-----------P-----------------------------------------------| </p>
     * <p>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;|---------------------|</p>
     * <p>|---------------------------------R-------------------------| </p>
     *
     * @param pos 当前mappedFile需要执行reput的起始位置
     * @return result
     */
    public SelectMappedBufferResult selectMappedBuffer(int pos) {
        int readPosition = getReadPosition();
        if(pos < readPosition && pos >= 0){
            if(isHold()){
                ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
                byteBuffer.position(pos);

                int size = readPosition - pos;
                ByteBuffer byteBufferNew = byteBuffer.slice();
                byteBufferNew.limit(size);
                return new SelectMappedBufferResult(this.fileFromOffset + pos, byteBufferNew, size, this);
            }
        }

        return null;
    }

    public boolean appendMessage(byte[] data) {
        int currentPos = this.wrotePosition.get();

        if((currentPos + data.length) <= this.fileSize){
            try {
                this.fileChannel.position(currentPos);
                this.fileChannel.write(ByteBuffer.wrap(data));
            } catch (IOException e) {
                log.error("error occurred when append message to mappedFile", e);
            }

            this.wrotePosition.addAndGet(data.length);
            return true;
        }
        return false;
    }


    public AppendMessageResult appendMessage(final MessageExtBrokerInner messageInner,final AppendMessageCallback cb) {
        return appendMessageInner(messageInner, cb);
    }

    public AppendMessageResult appendMessages(final MessageExtBatch messageExtBatch, final AppendMessageCallback cb){
        return appendMessageInner(messageExtBatch, cb);
    }

    public AppendMessageResult appendMessageInner(MessageExt messageExt, final AppendMessageCallback cb){
        assert messageExt != null;
        assert cb != null;

        int currentPos = wrotePosition.get();
        if(currentPos < this.fileSize){
            ByteBuffer byteBuffer = this.writeBuffer != null ? this.writeBuffer.slice() : this.mappedByteBuffer.slice();
            //记录当前mappedFile的位置 important
            byteBuffer.position(currentPos);
            AppendMessageResult result;
            if(messageExt instanceof MessageExtBrokerInner){
                result = cb.doAppend(this.getFileFromOffset(), byteBuffer, this.fileSize - currentPos, (MessageExtBrokerInner)messageExt);
            }else if(messageExt instanceof MessageExtBatch){
                result = cb.doAppend(this.getFileFromOffset(), byteBuffer, this.fileSize - currentPos, (MessageExtBatch)messageExt);
            }else{
                return new AppendMessageResult(AppendMessageStatus.UNKNOWN_ERROR);
            }

            this.wrotePosition.addAndGet(result.getWriteBytes());
            this.storeTimestamp = result.getStoreTimestamp();
            return result;
        }
        log.warn("");
        return new AppendMessageResult(AppendMessageStatus.UNKNOWN_ERROR);
    }

    /**
     * 从storePool初始化DirectByteBuffer
     * @return
     */
    public ByteBuffer borrowBuffer(){
        return null;
    }

    /**
     *
     * @return 返回有效数据的最大位置
     */
    public int getReadPosition(){
        return this.writeBuffer == null ? this.wrotePosition.get() : this.committedPosition.get();
    }

    /**
     * 根据刷盘类型和每次刷盘页大小进行预热
     * 为什么需要进行预热？
     * 预热时原理怎么解释？
     * @param flushDiskType 刷盘类型
     * @param pages
     */
    public void warmupMappedFile(FlushDiskType flushDiskType, int pages) {
        long beginTimestamp = System.currentTimeMillis();
        ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
        int flush = 0;
        long time = System.currentTimeMillis();
        //i表示mappedFile的大小，j表示OS_PAGECACHE的数量
        for(int i = 0, j = 0; i < this.fileSize; i += MappedFile.OS_PAGE_SIZE, j++){
            byteBuffer.put(i, (byte)0);
            //刷盘类型是同步刷盘，没写入pages个内存页就刷盘一次
            if(flushDiskType == FlushDiskType.SYNC_FLUSH){
               if((i / OS_PAGE_SIZE) - (flush / OS_PAGE_SIZE) >= pages){
                   flush= i;
                   mappedByteBuffer.force();
               }
            }

            if(j % 1000 == 0){
                log.info("j = {}, costTime: {}ms", j, System.currentTimeMillis() - time);
                time = System.currentTimeMillis();
                try {
                    Thread.sleep(0);
                } catch (InterruptedException e) {
                    log.warn("warm up interrupted");
                }
            }
        }

        //刷盘方式为同步刷盘
        if(flushDiskType == FlushDiskType.SYNC_FLUSH){
            log.info("mapped file warm up done, force to disk, mappedFile:{}, costTime:{}ms", this.fileName, System.currentTimeMillis() - beginTimestamp);
            this.mappedByteBuffer.force();
        }

        //mappedByteBuffer加锁
        this.mlock();
    }

    /**
     * 锁定内存
     */
    public void mlock(){
        final long beginTimestamp = System.currentTimeMillis();
        final long address = ((DirectBuffer) this.mappedByteBuffer).address();
        //将要锁定的内存地址
        Pointer pointer = new Pointer(address);
        {
            int ret = LibC.INSTANCE.mlock(pointer, new NativeLong(this.fileSize));
            log.info("mlock {}, {}, {}, ret = {}, costTime: {}ms", address, this.fileName, this.fileSize, ret, System.currentTimeMillis() - beginTimestamp);
        }

        {
            int ret = LibC.INSTANCE.madvise(pointer, new NativeLong(this.fileSize), LibC.MADV_WILLNEED);
            log.info("madvise {}, {}, {}, ret = {}, costTime: {}ms", address, this.fileName, this.fileSize, ret, System.currentTimeMillis() - beginTimestamp);
        }
    }

    public void munlock(){
        final long beginTimestamp = System.currentTimeMillis();
        final long address = ((DirectBuffer) this.mappedByteBuffer).address();
        Pointer pointer = new Pointer(address);
        int ret = LibC.INSTANCE.munlock(pointer, new NativeLong(this.fileSize));
        log.info("munlock {}, {}, {}, ret = {}, costTime: {}ms", address, this.fileName, this.fileSize, ret, System.currentTimeMillis() - beginTimestamp);
    }

    public static void ensureDirOk(final String dirName){
        if(dirName != null){
            File file = new File(dirName);
            if(!file.exists()){
                boolean result = file.mkdirs();
                log.info(dirName + " mkdir " + (result ? "OK" : "Failed"));
            }

        }
    }

    /**
     * 判断mappedFile是否已写满: 每个mappedFile文件的大小 == mappedFile写文件的位置
     * @return
     */
    public boolean isFull(){
        return this.fileSize == this.wrotePosition.get();
    }

    public int getCommittedPosition() {
        return committedPosition.get();
    }

    public void setCommittedPosition(int committedPosition){
        this.committedPosition.set(committedPosition);
    }

    public int getWrotePosition() {
        return wrotePosition.get();
    }

    public void setWrotePosition(int wrotePosition){
        this.wrotePosition.set(wrotePosition);
    }

    public long getFileFromOffset() {
        return this.fileFromOffset;
    }

    public void setFileFromOffset(long fileFromOffset) {
        this.fileFromOffset = fileFromOffset;
    }

    public int getFileSize() {
        return fileSize;
    }

    public void setFileSize(int fileSize) {
        this.fileSize = fileSize;
    }

    public long getStoreTimestamp() {
        return storeTimestamp;
    }

    public void setStoreTimestamp(long storeTimestamp) {
        this.storeTimestamp = storeTimestamp;
    }

    public int getFlushedPosition() {
        return flushedPosition.get();
    }

    public void setFlushedPosition(int flushedPosition){
        this.flushedPosition.set(flushedPosition);
    }

    public boolean isFirstCreateInQueue() {
        return firstCreateInQueue;
    }

    public void setFirstCreateInQueue(boolean firstCreateInQueue) {
        this.firstCreateInQueue = firstCreateInQueue;
    }

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

}
