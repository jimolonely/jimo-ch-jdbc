package com.jimo.ch.response;

import java.io.IOException;
import java.io.InputStream;

/**
 * @author jimo
 * @version 1.0.0
 * @date 2020/8/30 16:20
 */
public class StreamSplitter {
    private static final int BUF_LEN = 65535;
    private final InputStream delegate;
    private final byte sep;

    private byte[] buf;
    private int posRead;
    private int posNext;

    private int markedRead;
    private int markedNext;

    private boolean readOnce;
    private boolean closed;

    public StreamSplitter(InputStream delegate, byte sep) {
        this(delegate, sep, BUF_LEN);
    }

    public StreamSplitter(InputStream delegate, byte sep, int bufLen) {
        this.delegate = delegate;
        this.sep = sep;
        buf = new byte[bufLen];
    }

    public StreamSplitter(ByteFragment byteFragment, byte sep) {
        this.delegate = byteFragment.asStream();
        this.sep = sep;
        buf = new byte[byteFragment.length()];
        readOnce = true;
    }

//    public StreamSplitter(InputStream delegate, byte sep) {
//
//        this.delegate = delegate;
//        this.sep = sep;
//    }

    public ByteFragment next() throws IOException {
        if (posNext >= posRead) {
            int readBytes = readFromStream();
            if (readBytes <= 0) {
                return null;
            }
        }
        int positionSep;
        return null;
    }

    private int readFromStream() throws IOException {
        if (readOnce) {
            if (posRead >= buf.length) {
                return -1;
            } else {
                int read = delegate.read(buf, posRead, buf.length - posRead);
                if (read > 0) {
                    posRead += read;
                }
                return read;
            }
        } else {
            if (posRead >= buf.length) {
                shiftOrResize();
            }
            int read = delegate.read(buf, posRead, buf.length - posRead);
            if (read > 0) {
                posRead += read;
            }
            return read;
        }
    }

    private void shiftOrResize() {
        if (posNext > 0) {
            byte[] oldBuf = buf;
        }
    }

    public void mark() {
        markedRead = posRead;
        markedNext = posNext;
    }

    public void reset() {
        posRead = markedRead;
        posNext = markedNext;
    }

    public void close() throws IOException {
        closed = true;
        delegate.close();
    }

    public boolean isClose() {
        return closed;
    }
}
