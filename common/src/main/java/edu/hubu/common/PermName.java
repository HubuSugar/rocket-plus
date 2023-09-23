package edu.hubu.common;

/**
 * @author: sugar
 * @date: 2023/6/3
 * @description:
 */
public class PermName {
    public static final int PERM_INHERIT = 0x1;
    public static final int PERM_WRITABLE = 0x1 << 1;
    public static final int PERM_READABLE = 0x1 << 2;

    public static boolean isWritable(int perm){
        return (perm & PERM_WRITABLE) == PERM_WRITABLE;
    }

    public static boolean isReadable(int perm){
        return (perm & PERM_READABLE) == PERM_READABLE;
    }

    public static boolean isInherited(int perm){
        return (perm & PERM_INHERIT) == PERM_INHERIT;
    }
}
