package edu.hubu.client.common;

import java.util.Random;

/**
 * @author: sugar
 * @date: 2023/6/4
 * @description:
 */
public class ThreadLocalIndex {
    private final ThreadLocal<Integer> threadIndex = new ThreadLocal<>();
    private final Random random = new Random();

    public int getAndIncrement(){
        Integer index = threadIndex.get();

        if(index == null){
            index = Math.abs(random.nextInt());

            if(index < 0) index = 0;
            threadIndex.set(index);
        }

        index = Math.abs(index + 1);
        if(index < 0) index = 0;

        threadIndex.set(index);
        return index;
    }
}
