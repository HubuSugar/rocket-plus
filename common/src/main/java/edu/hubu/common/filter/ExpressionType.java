package edu.hubu.common.filter;

/**
 * @author: sugar
 * @date: 2023/10/30
 * @description:
 */
public class ExpressionType {
    public static final String TAG = "TAG";
    public static final String SQL92 = "SQL92";

    public static boolean isTagType(String type) {
        return type == null || "".equals(type) || TAG.equals(type);
    }
}
