package com.jimo.ch.util.guava;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * @author jimo
 * @version 1.0.0
 * @date 2020/8/30 9:01
 */
public class StreamUtils {

    private static final Logger log = LoggerFactory.getLogger(StreamUtils.class);

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
}
