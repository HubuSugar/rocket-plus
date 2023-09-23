package edu.hubu.common.sysFlag;


/**
 * @author: sugar
 * @date: 2023/7/9
 * @description: sysFlag字段的取值  （MessageExt对象）
 */
public class MessageSysFlag {
    public static final int TRANSACTION_NOT_TYPE = 0;
    public static final int FLAG_COMPRESSED = 0x1;
    public static final int FLAG_MULTI_TAGS = 0x1 << 1; //2
    public static final int TRANSACTION_PREPARE_TYPE = 0X1 << 2; //4
    public static final int TRANSACTION_COMMIT_TYPE = 0x2 << 2;  //8
    public static final int TRANSACTION_ROLLBACK_TYPE = 0x3 << 2; // 12
    public static final int FLAG_BORN_HOST_V6 = 0x1 << 4;
    public static final int FLAG_STORE_HOST_V6 = 0x1 << 5;

    public static int getTransactionValue(int sysFlag){
        return sysFlag & TRANSACTION_ROLLBACK_TYPE;
    }

}
