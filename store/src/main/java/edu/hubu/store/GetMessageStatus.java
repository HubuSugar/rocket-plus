package edu.hubu.store;

/**
 * @author: sugar
 * @date: 2023/11/26
 * @description:
 */
public enum GetMessageStatus {
    FOUND,
    NO_MATCHED_MSG,
    MSG_WAS_REMOVING,
    OFFSET_FOUND_NULL,
    OFFSET_OVERFLOW_BADLY,
    OFFSET_OVERFLOW_ONE,
    OFFSET_TOO_SMALL,
    NO_MATCHED_LOGIC_QUEUE,
    NO_MESSAGE_IN_QUEUE
}
