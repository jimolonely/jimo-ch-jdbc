package com.jimo.ch.util;

import java.util.ArrayList;
import java.util.List;

/**
 * @author jimo
 * @version 1.0.0
 * @date 2020/8/30 14:08
 */
public class Utils {

    public static boolean startsWithIgnoreCase(String s, String pattern) {
        return s.substring(0, pattern.length()).equalsIgnoreCase(pattern);
    }

    public static String retainUnquoted(String s, char quoteChar) {
        final StringBuilder sb = new StringBuilder();
        String[] split = splitWithoutEscaped(s, quoteChar, true);
        for (int i = 0; i < split.length; i++) {
            if ((i & 1) == 0) {
                sb.append(split[i]);
            }
        }
        return sb.toString();
    }

    private static String[] splitWithoutEscaped(String s, char quoteChar, boolean retainEmpty) {
        int len = s.length();
        if (len == 0) {
            return new String[0];
        }
        List<String> list = new ArrayList<>();
        int i = 0;
        int start = 0;
        boolean match = false;
        while (i < len) {
            if (s.charAt(i) == '\\') {
                match = true;
                i += 2;
            } else if (s.charAt(i) == quoteChar) {
                if (retainEmpty || match) {
                    list.add(s.substring(start, i));
                    match = false;
                }
                start = ++i;
            } else {
                match = true;
                i++;
            }
        }
        if (retainEmpty || match) {
            list.add(s.substring(start, i));
        }
        return list.toArray(new String[0]);
    }
}
