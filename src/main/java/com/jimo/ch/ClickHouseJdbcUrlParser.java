package com.jimo.ch;

import com.jimo.ch.setting.ClickHouseProperties;

import java.net.URISyntaxException;
import java.util.Properties;

/**
 * @author jimo
 * @version 1.0.0
 * @date 2020/8/29 16:21
 */
public class ClickHouseJdbcUrlParser {

    public static final String JDBC_PREFIX = "jdbc:";
    public static final String JDBC_CLICKHOUSE_PREFIX = JDBC_PREFIX + "clickhouse:";

    public static ClickHouseProperties parse(String jdbcUrl, Properties defaults) throws URISyntaxException {
        // TODO
        return null;
    }
}
