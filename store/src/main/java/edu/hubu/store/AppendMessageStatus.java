package edu.hubu.store;

/**
 * @author: sugar
 * @date: 2023/7/15
 * @description:
 */
public enum AppendMessageStatus {
    PUK_OK,
    END_OF_FILE,
    MESSAGE_SIZE_EXCEEDED,
    PROPERTIES_SIZE_EXCEEDED,
    UNKNOWN_ERROR
}
