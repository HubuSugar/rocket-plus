package edu.hubu.store;

import edu.hubu.common.utils.UtilAll;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * @author: sugar
 * @date: 2023/7/16
 * @description: mappedFile管理器
 */
@Slf4j
public class MappedFileQueue {

    private final String storePath;
    //每一个mappedFile文件的大小, 可能是commitlog的mappedFile也可能是consumeQueue的mappedFile
    private final int mappedFileSize;
    private final CopyOnWriteArrayList<MappedFile> mappedFiles = new CopyOnWriteArrayList<>();
    private final AllocateMappedFileService allocateMappedFileService;

    //刷盘进度
    private long flushedWhere = 0;
    private long committedWhere = 0;

    private volatile long storeTimestamp;

    public MappedFileQueue(String storePath, int mappedFileSize,
                           AllocateMappedFileService allocateMappedFileService) {
        this.storePath = storePath;
        this.mappedFileSize = mappedFileSize;
        this.allocateMappedFileService = allocateMappedFileService;
    }

    public boolean load() {
        File dir = new File(this.storePath);
        File[] files = dir.listFiles();
        if(files != null){
            Arrays.sort(files);
            for (File file : files) {
                if(file.length() != this.mappedFileSize){
                    log.error("file {} length = {} length not matched message store config value, please check it manually", file, file.length());
                    return false;
                }

                try{
                    MappedFile mappedFile = new MappedFile(file.getPath(), mappedFileSize);
                    mappedFile.setWrotePosition(this.mappedFileSize);
                    mappedFile.setFlushedPosition(this.mappedFileSize);
                    mappedFile.setCommittedPosition(this.mappedFileSize);

                    this.mappedFiles.add(mappedFile);
                    log.info("load {} Ok", file.getPath());
                } catch (IOException e) {
                    log.error("load file {} error", file, e);
                    return false;
                }
            }
        }

        return true;
    }

    /**
     * 获取第一个mappedFile
     * @return
     */
    public MappedFile getFirstMappedFile() {
        MappedFile mappedFileFirst = null;

        if (!this.mappedFiles.isEmpty()) {
            try {
                mappedFileFirst = this.mappedFiles.get(0);
            } catch (IndexOutOfBoundsException ignored) {

            } catch (Exception e) {
                log.error("get first mapped file exception", e);
            }
        }

        return mappedFileFirst;
    }

    /**
     * 获取mappedFileQueue集合的最后一个mappedFile
     * @return
     */
    public MappedFile getLastMappedFile(){
        MappedFile mappedFileLast = null;

        while(!this.mappedFiles.isEmpty()){
            try{
                mappedFileLast = this.mappedFiles.get(mappedFiles.size() - 1);
                break;
            }catch (IndexOutOfBoundsException e){
               //continue
            }catch (Exception e){
                log.error("get last mapped file exception", e);
                break;
            }
        }
        return mappedFileLast;
    }


    /**
     * commitlog中从mappedFileQueue尾部没有取到mappedFile
     * @param startOffset
     * @return
     */
    public MappedFile getLastMappedFile(final long startOffset){
        return getLastMappedFile(startOffset, true);
    }

    public MappedFile getLastMappedFile(final long startOffset, final boolean needCreated){
        long createOffset = -1;

        MappedFile lastMappedFile = getLastMappedFile();
        //最后一个mappedFile为空, 根据startOffset计算下一个mappedFile文件的起始位置
        if(lastMappedFile == null){
            createOffset = startOffset - (startOffset % this.mappedFileSize);
        }
        //最后一个mappedFile不为空，但是可以写的空间已经满了
        if(lastMappedFile != null && lastMappedFile.isFull()){
            createOffset = lastMappedFile.getFileFromOffset() + this.mappedFileSize;
        }

        if(createOffset != -1 && needCreated){
            //同时生成两个创建mappedFile的任务
            String nextFilePath = this.storePath + File.separator + UtilAll.offset2Filename(createOffset);
            //下下一个mappedFile的起始位置 = createOffset + mappedFileSize
            String nextNextFilePath = this.storePath + File.separator + UtilAll.offset2Filename(createOffset + this.mappedFileSize);

            MappedFile mappedFile = null;
            //如果AllocateMappedFileService存在那么通过mappedFile线程创建mappedFile， 否则直接通过new创建mappedFile
            if(this.allocateMappedFileService != null){
               mappedFile = allocateMappedFileService.putRequestAndReturnMappedFile(nextFilePath, nextNextFilePath, mappedFileSize);
            }else{
                try{
                    mappedFile = new MappedFile(nextFilePath, this.mappedFileSize);
                }catch (Exception e){
                    log.error("create mapped file handle exception", e);
                }
            }

            if (mappedFile != null) {
                if (this.mappedFiles.isEmpty()) {
                    mappedFile.setFirstCreateInQueue(true);
                }
                this.mappedFiles.add(mappedFile);

            }

            return mappedFile;
        }

        return lastMappedFile;
    }

    /**
     * 调用mappedFile的刷盘，更新刷盘偏移量
     * @param flushLeastPages
     * @return
     */
    public boolean flush(int flushLeastPages){
        boolean result = true;
        MappedFile mappedFile = this.findMappedFileByOffset(this.flushedWhere, this.flushedWhere == 0);
        if(mappedFile != null){
            long storeTimestamp = mappedFile.getStoreTimestamp();
            int offset = mappedFile.flush(flushLeastPages);
            long where = mappedFile.getFileFromOffset() + offset;
            result = (where == this.flushedWhere);
            this.flushedWhere = where;
            if(flushLeastPages == 0){
                this.storeTimestamp = storeTimestamp;
            }
        }
        return result;
    }

    /**
     * 根据偏移量查找mappedFile
     * 根据第一个和最后一个mappedFile的偏移量和mappedFileSize来确定偏移量位于哪一个mappedFile
     * @param offset 具体偏移量
     * @param returnFirstOnNotFound 未找到时是否返回第一个
     * @return mappedFile
     */
    public MappedFile findMappedFileByOffset(long offset, boolean returnFirstOnNotFound){
        try{
            MappedFile firstMappedFile = this.getFirstMappedFile();
            MappedFile lastMappedFile = this.getLastMappedFile();
            if(firstMappedFile != null && lastMappedFile != null){
                if(offset < firstMappedFile.getFileFromOffset() || offset >= lastMappedFile.getFileFromOffset() + this.mappedFileSize){
                    log.error("offset not matched request offset:{}, firstOffset: {}, lastOffset:{}, mappedFileSize:{}, mapped files count:{}",
                            offset, firstMappedFile.getFileFromOffset(), lastMappedFile.getFileFromOffset() + this.mappedFileSize, this.mappedFileSize, mappedFiles.size());
                }else{
                    //mapped file的位置
                    int index = (int) ((offset / this.mappedFileSize) - (firstMappedFile.getFileFromOffset() / this.mappedFileSize));
                    MappedFile  targetMappedFile = null;
                    try{
                       targetMappedFile = this.mappedFiles.get(index);
                    }catch (Exception ignored){
                    }

                    if(targetMappedFile != null && offset >= targetMappedFile.getFileFromOffset() && offset < targetMappedFile.getFileFromOffset() + this.mappedFileSize){
                        return targetMappedFile;
                    }

                    for (MappedFile mappedFile : this.mappedFiles) {
                        if(offset >= mappedFile.getFileFromOffset() && offset < mappedFile.getFileFromOffset() + this.mappedFileSize){
                            return mappedFile;
                        }
                    }
                }
                if(returnFirstOnNotFound){
                    return firstMappedFile;
                }
            }
        }catch (Exception e){
            log.error("find by mapped file by offset exception", e);
        }
        return null;
    }

    public long getMaxOffset() {
        MappedFile mappedFile = getLastMappedFile();
        if(mappedFile != null){
            return mappedFile.getFileFromOffset() + mappedFile.getReadPosition();
        }
        return 0;
    }

    public long getMinOffset() {
        MappedFile mappedFile = getFirstMappedFile();
        if(mappedFile != null){
            return mappedFile.getFileFromOffset();
        }
        return -1;
    }

    public long getFlushedWhere() {
        return flushedWhere;
    }

    public void setFlushedWhere(long flushedWhere) {
        this.flushedWhere = flushedWhere;
    }

    public long getCommittedWhere() {
        return committedWhere;
    }

    public void setCommittedWhere(long committedWhere) {
        this.committedWhere = committedWhere;
    }

    public long getStoreTimestamp() {
        return storeTimestamp;
    }

    public void setStoreTimestamp(long storeTimestamp) {
        this.storeTimestamp = storeTimestamp;
    }

    public int getMappedFileSize() {
        return mappedFileSize;
    }
}
