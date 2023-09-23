package edu.hubu.client.latency;

/**
 * @author: sugar
 * @date: 2023/6/4
 * @description:
 */
public interface LatencyFaultTolerance<T> {

    void updateFaultItem(String name, long currentLatency, long notAvailableDuration);

    boolean isAvailable(final T name);

    T pickOneAtLeast();

    void remove(T notBestBroker);
}
