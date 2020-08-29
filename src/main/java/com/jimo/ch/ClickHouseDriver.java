package com.jimo.ch;

import com.google.common.collect.MapMaker;
import com.jimo.ch.setting.ClickHouseConnectionSettings;
import com.jimo.ch.setting.ClickHouseProperties;
import com.jimo.ch.setting.ClickHouseQueryParam;
import com.jimo.ch.setting.DriverPropertyCreator;
import com.jimo.ch.util.LogProxy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
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

    public ClickHouseConnection connect(String url, ClickHouseProperties info) throws SQLException {
        if (!acceptsURL(url)) {
            return null;
        }
        logger.debug("Create Connection");
        final ClickHouseConnectionImpl connection = new ClickHouseConnectionImpl(url, info);
        registerConnection(connection);
        return LogProxy.wrap(ClickHouseConnection.class, connection);
    }

    private void registerConnection(ClickHouseConnectionImpl connection) {
        connections.put(connection, Boolean.TRUE);
    }

    @Override
    public boolean acceptsURL(String url) throws SQLException {
        return url.startsWith(ClickHouseJdbcUrlParser.JDBC_CLICKHOUSE_PREFIX);
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
        final Properties copy = new Properties(info);
        Properties properties;
        try {
            properties = ClickHouseJdbcUrlParser.parse(url, copy).asProperties();
        } catch (Exception e) {
            properties = copy;
            logger.error("parse url properties failed {}", url, e);
        }
        List<DriverPropertyInfo> result =
                new ArrayList<>(ClickHouseQueryParam.values().length + ClickHouseConnectionSettings.values().length);
        result.addAll(dumpProperties(ClickHouseQueryParam.values(), properties));
        result.addAll(dumpProperties(ClickHouseConnectionSettings.values(), properties));
        return result.toArray(new DriverPropertyInfo[0]);
    }

    private List<DriverPropertyInfo> dumpProperties(DriverPropertyCreator[] creators, Properties properties) {
        List<DriverPropertyInfo> result = new ArrayList<>(creators.length);
        for (DriverPropertyCreator creator : creators) {
            result.add(creator.createDriverPropertyInfo(properties));
        }
        return result;
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
