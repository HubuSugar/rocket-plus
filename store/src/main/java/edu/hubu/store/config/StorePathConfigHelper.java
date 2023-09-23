package edu.hubu.store.config;

import java.io.File;

/**
 * @author: sugar
 * @date: 2023/7/23
 * @description:
 */
public class StorePathConfigHelper {

    public static String getLockFilePath(String rootPath){
        return rootPath + File.separator + "lock";
    }

    public static String getStorePathConsumeQueue(final String rootPath){
        return rootPath + File.separator + "consumequeue";
    }

    public static String getStorePathConsumeQueueExt(final String rootPath){
        return rootPath + File.separator + "consumequeue_ext";
    }
}
