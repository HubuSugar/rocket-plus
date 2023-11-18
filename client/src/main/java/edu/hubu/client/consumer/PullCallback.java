package edu.hubu.client.consumer;

/**
 * @author: sugar
 * @date: 2023/11/18
 * @description:
 */
public interface PullCallback {

    void onSuccess(final PullResult pullResult);

    void onException(final Throwable e);

}
