package com.jimo.ch.util.guava;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * @author jimo
 * @version 1.0.0
 * @date 2020/8/30 9:01
 */
public class StreamUtils {

    public static final Charset UTF_8 = StandardCharsets.UTF_8;
    private static final Logger log = LoggerFactory.getLogger(StreamUtils.class);
    private static final int BUF_SIZE = 0x1000; // 4k

    public static void close(Closeable closeable) {
        if (closeable == null) {
            return;
        }
        try {
            closeable.close();
        } catch (IOException e) {
            log.error("cannot close stream:{}", e.getMessage());
        }
    }

    public static void close(ResultSet rs) {
        if (rs == null) {
            return;
        }
        try {
            rs.close();
        } catch (SQLException e) {
            log.error("cannot close resultSet: {}", e.getMessage());
        }
    }

    public static byte[] toByteArray(InputStream in) throws IOException {
        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        copy(in, out);
        return out.toByteArray();
    }

    private static long copy(InputStream in, OutputStream out) throws IOException {
        final byte[] buf = new byte[BUF_SIZE];
        long total = 0;
        while (true) {
            int r = in.read(buf);
            if (r == -1) {
                break;
            }
            out.write(buf, 0, r);
            total += r;
        }
        return total;
    }
}
