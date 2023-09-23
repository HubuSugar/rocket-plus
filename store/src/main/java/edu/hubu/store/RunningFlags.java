package edu.hubu.store;

/**
 * @author: sugar
 * @date: 2023/8/28
 * @description:
 */
public class RunningFlags {

    private static final int NOT_READABLE_BIT = 1;
    private static final int NOT_WRITABLE_BIT = 1 << 1;
    private static final int WRITE_LOGIC_QUEUE_ERROR_BIT = 2 << 1;
    private static final int WRITE_INDEX_FILE_BIT = 3 << 1;
    private static final int DISK_FULL_BIT = 4 << 1;

    private volatile int flagBits = 0;

    public RunningFlags() {
    }

    public int getFlagBits() {
        return flagBits;
    }

    public void setFlagBits(int flagBits) {
        this.flagBits = flagBits;
    }

    public boolean isReadable(){
        if((this.flagBits & NOT_READABLE_BIT) == 0){
            return true;
        }
        return false;
    }

    public boolean isWritable(){
        if((this.flagBits & (NOT_WRITABLE_BIT | WRITE_LOGIC_QUEUE_ERROR_BIT | DISK_FULL_BIT | WRITE_INDEX_FILE_BIT)) == 0){
            return true;
        }
        return false;
    }

    /**
     * 对于consume queue，仅需要忽略磁盘写满状态
     * @return consume Queue是否可写
     */
    public boolean isCQWritable(){
        if((this.flagBits & (NOT_WRITABLE_BIT | WRITE_LOGIC_QUEUE_ERROR_BIT | WRITE_INDEX_FILE_BIT)) == 0){
            return true;
        }
        return false;
    }

    public void makeLogicQueueError(){
        this.flagBits |= WRITE_LOGIC_QUEUE_ERROR_BIT;
    }
}
