package edu.hubu.common.topic;

/**
 * @author: sugar
 * @date: 2024/7/27
 * @description:
 */
public class TopicSysFlag {

    private static final int FLAG_UNIT = 0x01 << 0;
    private static final int FLAG_UNIT_SUB = 0x01 << 1;

    public static int buildSysFlag(final boolean unit, final boolean unitSub){
        int sysFlag = 0;

        if(unit){
            sysFlag |= FLAG_UNIT;
        }

        if(unitSub){
            sysFlag |= FLAG_UNIT_SUB;
        }

        return sysFlag;
    }
}
