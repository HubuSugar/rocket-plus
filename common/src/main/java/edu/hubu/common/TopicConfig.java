package edu.hubu.common;

/**
 * @author: sugar
 * @date: 2023/6/11
 * @description:
 */
public class TopicConfig {
    private static final String SPLIT = " ";
    private static final int defaultTopicReadQueueNums = 16;
    private static final int defaultTopicWriteQueueNums = 16;
    private String topicName;
    private int readQueueNums = defaultTopicReadQueueNums;
    private int writeQueueNums = defaultTopicWriteQueueNums;
    private int perm = PermName.PERM_WRITABLE | PermName.PERM_READABLE;

    private int topicSysFlag = 0;
    private boolean order = false;
    private TopicFilterType topicFilterType = TopicFilterType.SINGLE_TAG;

    public TopicConfig() {
    }

    public TopicConfig(String topicName) {
        this.topicName = topicName;
    }

    public TopicConfig(String topicName, int readQueueNums, int writeQueueNums, int perm) {
        this.topicName = topicName;
        this.readQueueNums = readQueueNums;
        this.writeQueueNums = writeQueueNums;
        this.perm = perm;
    }

    public String encode(){
        return topicName + SPLIT +
                readQueueNums + SPLIT +
                writeQueueNums + SPLIT +
                perm + SPLIT +
                topicFilterType;
    }

    public boolean decode(final String in){
        String[] items = in.split(SPLIT);
        if(items.length == 5){
            this.topicName = items[0];
            this.readQueueNums = Integer.parseInt(items[1]);
            this.writeQueueNums = Integer.parseInt(items[2]);
            this.perm = Integer.parseInt(items[3]);
            this.topicFilterType = TopicFilterType.valueOf(items[4]);
            return true;
        }
        return false;
    }


    public String getTopicName() {
        return topicName;
    }

    public void setTopicName(String topicName) {
        this.topicName = topicName;
    }

    public int getReadQueueNums() {
        return readQueueNums;
    }

    public void setReadQueueNums(int readQueueNums) {
        this.readQueueNums = readQueueNums;
    }

    public int getWriteQueueNums() {
        return writeQueueNums;
    }

    public void setWriteQueueNums(int writeQueueNums) {
        this.writeQueueNums = writeQueueNums;
    }

    public int getPerm() {
        return perm;
    }

    public void setPerm(int perm) {
        this.perm = perm;
    }

    public int getTopicSysFlag() {
        return topicSysFlag;
    }

    public void setTopicSysFlag(int topicSysFlag) {
        this.topicSysFlag = topicSysFlag;
    }

    public boolean isOrder() {
        return order;
    }

    public void setOrder(boolean order) {
        this.order = order;
    }

    public TopicFilterType getTopicFilterType() {
        return topicFilterType;
    }

    public void setTopicFilterType(TopicFilterType topicFilterType) {
        this.topicFilterType = topicFilterType;
    }
}
