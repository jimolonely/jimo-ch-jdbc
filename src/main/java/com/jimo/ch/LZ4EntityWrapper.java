package com.jimo.ch;

import org.apache.http.HttpEntity;
import org.apache.http.entity.AbstractHttpEntity;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * @author jimo
 * @version 1.0.0
 * @date 2020/8/30 15:37
 */
public class LZ4EntityWrapper extends AbstractHttpEntity {

    public LZ4EntityWrapper(HttpEntity entity, int maxCompressBufferSize) {

    }

    @Override
    public boolean isRepeatable() {
        return false;
    }

    @Override
    public long getContentLength() {
        return 0;
    }

    @Override
    public InputStream getContent() throws IOException, UnsupportedOperationException {
        return null;
    }

    @Override
    public void writeTo(OutputStream outstream) throws IOException {

    }

    @Override
    public boolean isStreaming() {
        return false;
    }
}
