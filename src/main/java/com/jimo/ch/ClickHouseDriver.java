package com.jimo.ch;

import com.google.common.collect.MapMaker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.Properties;
import java.util.concurrent.ConcurrentMap;

/**
 * jdbc:clickhouse://host:port
 *
 * @author jimo
 * @version 1.0.0
 * @date 2020/8/29 16:11
 */
public class ClickHouseDriver implements Driver {

    private static final Logger logger = LoggerFactory.getLogger(ClickHouseDriver.class);

    private static final ConcurrentMap<ClickHouseConnectionImpl, Boolean> connections =
            new MapMaker().weakKeys().makeMap();

    static {
        final ClickHouseDriver driver = new ClickHouseDriver();
        try {
            DriverManager.registerDriver(driver);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        logger.info("Jimo ClickHouse Driver registered");
    }

    @Override
    public Connection connect(String url, Properties info) throws SQLException {
        return null;
    }

    @Override
    public boolean acceptsURL(String url) throws SQLException {
        return url.startsWith(ClickHouseJdbcUrlParser.JDBC_CLICKHOUSE_PREFIX);
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
        return new DriverPropertyInfo[0];
    }

    @Override
    public int getMajorVersion() {
        return 0;
    }

    @Override
    public int getMinorVersion() {
        return 0;
    }

    @Override
    public boolean jdbcCompliant() {
        return false;
    }

    @Override
    public java.util.logging.Logger getParentLogger() throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException();
    }
}
