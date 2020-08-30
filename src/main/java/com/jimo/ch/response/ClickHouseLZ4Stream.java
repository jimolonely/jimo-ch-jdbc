package com.jimo.ch.response;

import java.io.IOException;
import java.io.InputStream;

/**
 * @author jimo
 * @version 1.0.0
 * @date 2020/8/30 13:04
 */
public class ClickHouseLZ4Stream extends InputStream {

    public ClickHouseLZ4Stream(InputStream is) {

    }

    @Override
    public int read() throws IOException {
        return 0;
    }
}
