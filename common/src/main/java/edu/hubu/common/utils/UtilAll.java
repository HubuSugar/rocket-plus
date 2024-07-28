package edu.hubu.common.utils;

import java.nio.charset.StandardCharsets;
import java.text.NumberFormat;
import java.util.zip.CRC32;

/**
 * @author: sugar
 * @date: 2023/7/17
 * @description:
 */
public class UtilAll {
    private static final char[] HEX_ARRAY = "0123456789ABCDEF".toCharArray();

    public static String offset2Filename(final long offset){
        final NumberFormat nf = NumberFormat.getInstance();
        nf.setMinimumIntegerDigits(20);
        nf.setMaximumFractionDigits(0);
        nf.setGroupingUsed(false);
        return nf.format(offset);
    }

    public static String byte2String(byte[] array) {
        char[] hexString = new char[array.length * 2];
        for(int i = 0; i < array.length;i++){
            int v = array[i] & 0xFF;
            hexString[2 * i] = HEX_ARRAY[v >>> 4];
            hexString[2 * i + 1] = HEX_ARRAY[ v & 0x0F];
        }
        return new String(hexString);
    }

    public static long computeElapsedTimeMillis(long beginTime){
        return System.currentTimeMillis() - beginTime;
    }

    public static int crc32(byte[] body){
        if(body != null){
            return crc32(body, 0, body.length);
        }
        return 0;
    }

    public static int crc32(byte[] body, int offset, int len){
        CRC32 crc32 = new CRC32();
        crc32.update(body, offset, len);
        return (int) (crc32.getValue() & 0x7FFFFFFF);
    }
}
