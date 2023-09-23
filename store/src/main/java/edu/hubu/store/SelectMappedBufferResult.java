package edu.hubu.store;

import java.nio.ByteBuffer;

/**
 * @author: sugar
 * @date: 2023/8/19
 * @description:
 */
public class SelectMappedBufferResult {

    private final long startOffset;
    //当前需要reput的byteBuffer
    private final ByteBuffer byteBuffer;
    private int size;
    //当前需要reput的mappedFile
    private MappedFile mappedFile;

    public SelectMappedBufferResult(long startOffset, ByteBuffer byteBuffer, int size, MappedFile mappedFile) {
        this.startOffset = startOffset;
        this.byteBuffer = byteBuffer;
        this.size = size;
        this.mappedFile = mappedFile;
    }

    public synchronized void release(){
        if(this.mappedFile != null){
            this.mappedFile.release();
            this.mappedFile = null;
        }
    }

    public ByteBuffer getByteBuffer() {
        return byteBuffer;
    }

    public int getSize() {
        return size;
    }

    public void setSize(int size) {
        this.size = size;
        byteBuffer.limit(size);
    }

    public MappedFile getMappedFile() {
        return mappedFile;
    }

    public void setMappedFile(MappedFile mappedFile) {
        this.mappedFile = mappedFile;
    }

    public long getStartOffset() {
        return startOffset;
    }
}
