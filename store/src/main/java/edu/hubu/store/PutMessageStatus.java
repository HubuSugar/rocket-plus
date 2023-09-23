package edu.hubu.store;

/**
 * @author: sugar
 * @date: 2023/7/15
 * @description:
 */
public enum PutMessageStatus {
    PUT_OK,
    FLUSH_DISK_TIMEOUT,
    FLUSH_SLAVE_TIMEOUT,
    SLAVE_NOT_AVAILABLE,
    SERVICE_NOT_AVAILABLE,
    CREATE_MAPPED_FILE_FAILED,
    MESSAGE_ILLEGAL,
    PROPERTIES_SIZE_EXCEED,
    OS_PAGECACHE_BUSY,
    UNKNOWN_ERROR

}
