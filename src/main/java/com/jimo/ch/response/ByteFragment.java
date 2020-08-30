package com.jimo.ch.response;

import com.jimo.ch.util.guava.StreamUtils;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * @author jimo
 * @version 1.0.0
 * @date 2020/8/30 16:22
 */
public class ByteFragment {

    protected final byte[] buf;
    protected final int start;
    protected final int len;
    private static final ByteFragment EMPTY = new ByteFragment(new byte[0], 0, 0);

    public ByteFragment(byte[] buf, int start, int len) {
        this.buf = buf;
        this.start = start;
        this.len = len;
    }

    public static ByteFragment fromString(String str) {
        byte[] bytes = str.getBytes(StreamUtils.UTF_8);
        return new ByteFragment(bytes, 0, bytes.length);
    }

    public String asString(boolean unescape) {
        if (unescape) {
            return isNull() ? null : new String(unescape(), StreamUtils.UTF_8);
        } else {
            return asString();
        }
    }

    public ByteFragment[] split(byte sep) {
        StreamSplitter ss = new StreamSplitter(this, sep);
        int c = count(sep) + 1;
        ByteFragment[] res = new ByteFragment[c];
        try {
            int i = 0;
            ByteFragment next;
            while ((next = ss.next()) != null) {
                res[i++] = next;
            }
        } catch (IOException ignore) {
        }
        if (res[c - 1] == null) {
            res[c - 1] = ByteFragment.EMPTY;
        }
        return res;
    }

    private int count(byte sep) {
        int res = 0;
        for (int i = start; i < start + len; i++) {
            if (buf[i] == sep) {
                res++;
            }
        }
        return res;
    }

    public int length() {
        return len;
    }

    public String asString() {
        return new String(buf, start, len, StreamUtils.UTF_8);
    }

    public boolean isNull() {
        return len == 2 && buf[start] == '\\' && buf[start + 1] == 'N';
    }

    public byte[] unescape() {
        // TODO
        return null;
    }

    public InputStream asStream() {
        return new ByteArrayInputStream(buf, start, len);
    }
}
