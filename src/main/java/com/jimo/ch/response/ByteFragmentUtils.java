package com.jimo.ch.response;

/**
 * @author jimo
 * @version 1.0.0
 * @date 2020/8/30 17:29
 */
public class ByteFragmentUtils {

    public static int parseInt(ByteFragment s) {
        if (s == null) {
            throw new NumberFormatException("null");
        }
        if (s.isNull()) {
            return 0;
        }
        // TODO
        return 0;
    }
}
