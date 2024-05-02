package edu.hubu.store;

import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;

/**
 * @author: sugar
 * @date: 2023/12/17
 * @description:
 */
public class StoreUtil {
    public static final long TOTAL_PHYSICAL_MEMORY_SIZE = getTotalPhysicalMemorySize();

    public static long getTotalPhysicalMemorySize(){
        long physicalTotal = 1024 * 1024 * 1024 * 24L;  //24G
        OperatingSystemMXBean mxBean = ManagementFactory.getOperatingSystemMXBean();
        if(mxBean instanceof com.sun.management.OperatingSystemMXBean){
            physicalTotal = ((com.sun.management.OperatingSystemMXBean)mxBean).getTotalPhysicalMemorySize();
        }
        return physicalTotal;
    }
}
