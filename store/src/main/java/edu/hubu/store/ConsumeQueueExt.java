package edu.hubu.store;

import java.io.File;
import java.nio.ByteBuffer;

/**
 * @author: sugar
 * @date: 2023/8/27
 * @description:
 */
public class ConsumeQueueExt {

    public static final int END_BLANK_DATA_LENGTH = 4;
    public static final long MAX_ADDR = Integer.MIN_VALUE - 1L;
    public static final long MAX_REAL_OFFSET = MAX_ADDR - Long.MIN_VALUE;

    private final MappedFileQueue mappedFileQueue;
    private final String topic;
    private final int queueId;
    private final String storePath;
    private final int mappedFileSize;

    private ByteBuffer tempContainer;

    public ConsumeQueueExt(String topic, int queueId, String storePath, int mappedFileSize, final int bitmapLength) {
        this.topic = topic;
        this.queueId = queueId;
        this.storePath = storePath;
        this.mappedFileSize = mappedFileSize;

        String queueDir = this.storePath + File.separator + topic + File.separator + queueId;
        this.mappedFileQueue = new MappedFileQueue(storePath, mappedFileSize, null);

        if(bitmapLength > 0){
            this.tempContainer = ByteBuffer.allocate(bitmapLength / Byte.SIZE);
        }
    }

    public static boolean isExtAddr(long tagsCode){
        return tagsCode <= MAX_ADDR;
    }

    public boolean flush(int flushLeastPages){
        return true;
    }

    public boolean load() {
        return true;
    }

    public long put(CqUnitExt cqUnitExt) {
        return 1;
    }

    public void recover() {

    }

    public void truncateByMaxAddress(long maxExtAddr) {

    }

    public void destroy() {

    }

    public void truncateByMinAddress(long minExtAddr) {

    }


    public static class CqUnitExt{
        // size + msg time + tag code + bitmap size
        public static final short MIN_EXT_UNIT_SIZE = 2 + 8 * 2 + 2;
        public static final short MAX_EXT_UNIT_SIZE = Short.MAX_VALUE;

        private short size;

        private long tagsCode;

        private long msgStoreTime;

        private short bitmapSize;

        private byte[] filterBitmap;

        public CqUnitExt() {
        }

        public CqUnitExt(Long tagsCode, long msgStoreTime, byte[] filterBitmap) {
            this.tagsCode = tagsCode == null ? 0  : tagsCode;
            this.msgStoreTime = msgStoreTime;
            this.filterBitmap = filterBitmap;
            this.bitmapSize = (short) (this.filterBitmap == null ? 0 : this.filterBitmap.length);
            this.size = (short) (MIN_EXT_UNIT_SIZE + this.bitmapSize);
        }

        public short getSize() {
            return size;
        }

        public void setSize(short size) {
            this.size = size;
        }

        public long getTagsCode() {
            return tagsCode;
        }

        public void setTagsCode(long tagsCode) {
            this.tagsCode = tagsCode;
        }

        public long getMsgStoreTime() {
            return msgStoreTime;
        }

        public void setMsgStoreTime(long msgStoreTime) {
            this.msgStoreTime = msgStoreTime;
        }

        public short getBitmapSize() {
            return bitmapSize;
        }

        public void setBitmapSize(short bitmapSize) {
            this.bitmapSize = bitmapSize;
        }

        public byte[] getFilterBitmap() {
            return filterBitmap;
        }

        public void setFilterBitmap(byte[] filterBitmap) {
            this.filterBitmap = filterBitmap;
        }
    }

}
