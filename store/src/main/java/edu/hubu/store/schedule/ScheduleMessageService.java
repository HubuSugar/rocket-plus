package edu.hubu.store.schedule;

import edu.hubu.store.DefaultMessageStore;

/**
 * @author: sugar
 * @date: 2023/9/17
 * @description:
 */
public class ScheduleMessageService{

    private final DefaultMessageStore defaultMessageStore;
    private int maxDelayLevel;

    public ScheduleMessageService(DefaultMessageStore defaultMessageStore) {
        this.defaultMessageStore = defaultMessageStore;
    }

    public boolean load(){
        return this.parseDelayLevel();
    }

    public boolean parseDelayLevel(){
        this.maxDelayLevel = 18;
        return true;
    }

    public long computeDeliverTimestamp(int delayLevel, long storeTimestamp) {
        return 0;
    }



    public int getMaxDelayLevel() {
        return maxDelayLevel;
    }

    public DefaultMessageStore getDefaultMessageStore() {
        return defaultMessageStore;
    }

}
