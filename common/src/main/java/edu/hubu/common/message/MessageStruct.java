package edu.hubu.common.message;

import sun.security.provider.PolicySpiFile;

/**
 * @author: sugar
 * @date: 2024/5/2
 * @description:
 */
public class MessageStruct {
    public static final int MSG_TOTAL_SIZE = 4;
    public static final int MSG_MAGIC_CODE = 4;
    public static final int MSG_BODY_CRC = 4;
    public static final int MSG_QUEUE_ID = 4;
    public static final int MSG_FLAG = 4;
    public static final int MSG_QUEUE_OFFSET = 8;
    public static final int MSG_PHYSICAL_OFFSET = 8;
    public static final int MSG_SYS_FLAG = 4;
    public static final int MSG_BORN_TIMESTAMP = 8;
    public static final int MSG_STORE_TIMESTAMP = 8;
    public static final int MSG_RECONSUME_TIMES = 4;
    public static final int MSG_PREPARE_TRANS_OFFSET = 8;

}
