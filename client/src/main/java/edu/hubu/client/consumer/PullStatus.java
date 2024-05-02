package edu.hubu.client.consumer;

/**
 * @author: sugar
 * @date: 2023/11/7
 * @description:
 */
public enum PullStatus {

    FOUND,
    NO_NEW_MSG,
    NO_MATCHED_MSG,
    OFFSET_ILLEGAL

}
