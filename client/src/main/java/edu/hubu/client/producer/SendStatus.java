package edu.hubu.client.producer;

/**
 * @author: sugar
 * @date: 2023/7/9
 * @description:
 */
public enum SendStatus {
    SEND_OK,
    FLUSH_DISK_TIMEOUT,
    FLUSH_SLAVE_TIMEOUT,
    SLAVE_NOT_AVAILABLE;
}
