package edu.hubu.common.sysFlag;

/**
 * @author: sugar
 * @date: 2023/11/18
 * @description:
 */
public class PullSysFlag {

    private static final int FLAG_COMMIT_OFFSET = 0x1;
    private static final int FLAG_SUSPEND = 0x1 << 1;
    private static final int FLAG_SUBSCRIPTION = 0x1 << 2;
    private static final int FLAG_CLASS_FILTER = 0x1 << 3;
    private static final int FLAG_LITE_PULL_MESSAGE = 0x1 << 4;


    public static int buildSysFlag(final boolean commitOffset, final boolean suspend, boolean subscription, boolean classFilter) {
        int flag = 0;
        if(commitOffset){
            flag |= FLAG_COMMIT_OFFSET;
        }
        if(suspend){
            flag |= FLAG_SUSPEND;
        }
        if(subscription){
            flag |= FLAG_SUBSCRIPTION;
        }
        if(classFilter){
            flag |= FLAG_CLASS_FILTER;
        }
        return flag;
    }

    public static int buildSysFlag(final boolean commitOffset, final boolean suspend, boolean subscription, boolean classFilter, boolean litePull) {
        int flag = buildSysFlag(commitOffset, suspend, subscription, classFilter);
        if(litePull){
            flag |= FLAG_LITE_PULL_MESSAGE;
        }
        return flag;
    }

    public static int clearCommitlogOffsetFlag(final int sysFlag) {
        return sysFlag & (~FLAG_COMMIT_OFFSET);
    }

    public static boolean hasClassFilterFlag(final int sysFlag) {
        return (sysFlag & FLAG_CLASS_FILTER) == FLAG_CLASS_FILTER;
    }
}
