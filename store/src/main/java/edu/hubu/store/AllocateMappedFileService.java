package edu.hubu.store;

import edu.hubu.common.ServiceThread;
import edu.hubu.store.config.BrokerRole;
import edu.hubu.store.config.MessageStoreConfig;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.IOException;
import java.util.ServiceLoader;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * @author: sugar
 * @date: 2023/7/15
 * @description: 创建mappedFile服务
 */
@Slf4j
public class AllocateMappedFileService extends ServiceThread {

    private static final long awaitTimeout = 1000 * 5;
    private final DefaultMessageStore messageStore;
    private final ConcurrentHashMap<String, AllocateRequest> requestTable = new ConcurrentHashMap<>();
    private final PriorityBlockingQueue<AllocateRequest> requestQueue = new PriorityBlockingQueue<>();
    private volatile boolean hasException = false;

    public AllocateMappedFileService(DefaultMessageStore messageStore) {
        this.messageStore = messageStore;
    }

    public MappedFile putRequestAndReturnMappedFile(String nextFilePath, String nextNextFilePath, int mappedFileSize) {
        int canSubmitRequest = 2;
        //判断
        if(messageStoreConfig().isEnableTransientStorePool()){
            if(messageStoreConfig().isFastFailIfNoBufferInPool() &&
              BrokerRole.SLAVE != messageStoreConfig().getBrokerRole()){
                canSubmitRequest = this.messageStore.getTransientStorePool().availableBufferNums() - this.requestQueue.size();
            }
        }

        AllocateRequest nextRequest = new AllocateRequest(nextFilePath, mappedFileSize);
        boolean nextPutOk = this.requestTable.putIfAbsent(nextFilePath, nextRequest) == null;
        if(nextPutOk){
            if(canSubmitRequest <= 0){
                log.warn("[notify me] transient pool is not enough, so create mapped file failed, queue size: {}, store size:{}", this.requestQueue.size(), this.messageStore.getTransientStorePool().availableBufferNums());
                //可容纳的请求数不足时，将之前的请求从requestTable中移除
                this.requestTable.remove(nextFilePath);
                return null;
            }
            boolean offerOk = requestQueue.offer(nextRequest);
            if(!offerOk){
                log.warn("never expected here offer a request to pre allocate queue failed");
            }
            canSubmitRequest--;
        }

        //第二个创建mappedFile请求
        AllocateRequest nextNextRequest = new AllocateRequest(nextNextFilePath, mappedFileSize);
        boolean nextNextPutOk = this.requestTable.putIfAbsent(nextNextFilePath, nextNextRequest) == null;
        if(nextNextPutOk){
            if(canSubmitRequest <= 0){
                log.warn("[notify me] transient pool is not enough, so skip pre allocate mapped file, queue size: {}, store size:{}", this.requestQueue.size(), this.messageStore.getTransientStorePool().availableBufferNums());
                this.requestTable.remove(nextNextFilePath);
            }else{
                boolean offerOk = this.requestQueue.offer(nextNextRequest);
                if(!offerOk){
                    log.warn("never expected here add a request to preallocate queue failed");
                }
            }
        }

        if(hasException){
            log.warn(this.getServiceName() + "has exception, so return null");
            return null;
        }

        AllocateRequest request = this.requestTable.get(nextFilePath);
        try {
            if (request != null) {
                boolean awaitOk = request.getCountDownLatch().await(awaitTimeout, TimeUnit.MILLISECONDS);
                if (!awaitOk) {
                    log.warn("create mapped file timeout");
                    return null;
                }else{
                    this.requestTable.remove(nextFilePath);
                    return request.getMappedFile();
                }
            }else{
                log.warn("find preallocate request failed this will never happen");
            }
        } catch (InterruptedException e) {
            log.error("wait allocate request interrupted ", e);
        }

        return null;
    }

    private MessageStoreConfig messageStoreConfig(){
        return this.messageStore.getMessageStoreConfig();
    }

    @Override
    public String getServiceName() {
        return AllocateMappedFileService.class.getSimpleName();
    }

    @Override
    public void run() {
        log.info(" {} started.",  getServiceName());
        while(!isStopped() && this.mappedFileOperation()){

        }
        log.info(" {} stopped", getServiceName());
    }

    private boolean mappedFileOperation(){
        boolean isSuccess = false;
        AllocateRequest request = null;
        try {
            request = this.requestQueue.take();
            AllocateRequest allocateRequest = this.requestTable.get(request.getFilePath());
            if(null == allocateRequest){
                log.warn("this mmap request expired, maybe cause timeout, filepath: {}, fileSize: {}", request.getFilePath(), request.getFileSize());
                return true;
            }

            if(request != allocateRequest){
                log.warn("never expect here, maybe cause timeout, request: {}, allocateRequest:{} ", request, allocateRequest);
                return true;
            }

            if(request.getMappedFile() == null){
                long beginTime = System.currentTimeMillis();
                MappedFile mappedFile;
                if(messageStoreConfig().isEnableTransientStorePool()){
                    try{
                        mappedFile = ServiceLoader.load(MappedFile.class).iterator().next();
                        mappedFile.init(request.getFilePath(), request.getFileSize(), messageStore.getTransientStorePool());
                    }catch (RuntimeException e){
                        log.warn("use default implements");
                        mappedFile = new MappedFile(request.filePath, request.getFileSize(), messageStore.getTransientStorePool());
                    }
                }else{
                    mappedFile = new MappedFile(request.filePath, request.getFileSize());
                }

                long costTime = System.currentTimeMillis() - beginTime;
                if(costTime > 10) {
                    log.warn("allocate mapped file cost: {}ms, request queue size: {}", costTime, requestQueue.size());
                }

                //预热mappedFile
                if(mappedFile.getFileSize() > messageStoreConfig().getMappedFileSizeCommitlog() && messageStoreConfig().isWarmupMappedFileEnable()){
                    mappedFile.warmupMappedFile(messageStoreConfig().getFlushDiskType() , messageStoreConfig().getFlushLeastPagesWhenWarmup());
                }

                request.setMappedFile(mappedFile);
                this.hasException = false;
                isSuccess = true;
            }
        } catch (InterruptedException e) {
            log.error("take request from request queue failed", e);
            this.hasException = true;
            return false;
        }catch (IOException e){
            log.error("exception", e);
            hasException = true;
            if(request != null){
                this.requestQueue.offer(request);
                try{
                    Thread.sleep(1);
                }catch (InterruptedException ignored){

                }
            }
        }finally {
            if(request != null && isSuccess){
                request.getCountDownLatch().countDown();
            }
        }
        return true;
    }


    static class AllocateRequest implements Comparable<AllocateRequest>{
        //文件全路径
        private String filePath;
        private int fileSize;
        private CountDownLatch countDownLatch = new CountDownLatch(1);
        private volatile MappedFile mappedFile;

        public AllocateRequest(String filePath, int fileSize) {
            this.filePath = filePath;
            this.fileSize = fileSize;
        }

        @Override
        public int compareTo(AllocateRequest o) {
            if(o.fileSize > this.fileSize){
                return 1;
            }
            if(o.fileSize < this.fileSize){
                return -1;
            }
            int mIndex = this.filePath.lastIndexOf(File.separator);
            int oIndex = o.filePath.lastIndexOf(File.separator);
            long mName = Long.parseLong(this.filePath.substring(mIndex + 1));
            long oName = Long.parseLong(o.filePath.substring(oIndex + 1));
            return Long.compare(mName, oName);
        }

        public String getFilePath() {
            return filePath;
        }

        public void setFilePath(String filePath) {
            this.filePath = filePath;
        }

        public int getFileSize() {
            return fileSize;
        }

        public void setFileSize(int fileSize) {
            this.fileSize = fileSize;
        }

        public CountDownLatch getCountDownLatch() {
            return countDownLatch;
        }

        public void setCountDownLatch(CountDownLatch countDownLatch) {
            this.countDownLatch = countDownLatch;
        }

        public MappedFile getMappedFile() {
            return mappedFile;
        }

        public void setMappedFile(MappedFile mappedFile) {
            this.mappedFile = mappedFile;
        }


    }
}
