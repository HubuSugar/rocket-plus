package edu.hubu.store.util;


import com.sun.jna.*;
import edu.hubu.remoting.netty.RemotingUtil;

/**
 * @author: sugar
 * @date: 2023/7/23
 * @description: JNA调用系统底层的c或者c++方法，
 * 对mappedFile预热加锁的相关解释： https://juejin.cn/post/7169913280654213151
 */
public interface LibC extends Library {

    LibC INSTANCE = (LibC) Native.loadLibrary(RemotingUtil.isWindowsPlatform() ? "msvcrt" : "c", LibC.class);

    //将被使用的内存
    int MADV_WILLNEED = 3;
    //不会被使用到的内存
    int MADV_DONTNEED = 4;

    int MCL_CURRENT = 1;
    int MCL_FUTURE = 2;
    int MCL_ONFAULT = 4;

    /**
     * 对预热后的mappedFile加锁
     */
    int mlock(Pointer var1, NativeLong var2);

    int munlock(Pointer var1, NativeLong var2);

    //对系统的内存优化给出建议， var3建议的级别

    int madvise(Pointer var1, NativeLong var2, int var3);

    Pointer memset(Pointer p, int v, long len);

    int mlockall(int flags);

    int msync(Pointer p, NativeLong length, int flags);
}
