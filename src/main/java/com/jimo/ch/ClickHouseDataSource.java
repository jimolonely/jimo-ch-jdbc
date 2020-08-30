package com.jimo.ch;

import com.jimo.ch.setting.ClickHouseProperties;

import javax.sql.DataSource;
import java.io.PrintWriter;
import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.logging.Logger;

/**
 * @author jimo
 * @version 1.0.0
 * @date 2020/8/30 8:15
 */
public class ClickHouseDataSource implements DataSource {

    protected final ClickHouseDriver driver = new ClickHouseDriver();
    protected final String url;
    protected PrintWriter printWriter;
    protected int loginTimeoutSeconds = 0;
    private ClickHouseProperties properties;

    public ClickHouseDataSource(String url) {
        this(url, new ClickHouseProperties());
    }

    public ClickHouseDataSource(String url, Properties info) {
        this(url, new ClickHouseProperties(info));
    }

    public ClickHouseDataSource(String url, ClickHouseProperties properties) {
        if (url == null) {
            throw new IllegalArgumentException("Incorrect ClickHouse jdbc url. It must not be null");
        }
        this.url = url;

        try {
            this.properties = ClickHouseJdbcUrlParser.parse(url, properties.asProperties());
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException(e);
        }
    }

    public String getHost() {
        return properties.getHost();
    }

    public ClickHouseProperties getProperties() {
        return properties;
    }

    @Override
    public Connection getConnection() throws SQLException {
        return driver.connect(url, properties);
    }

    @Override
    public Connection getConnection(String username, String password) throws SQLException {
        return driver.connect(url, properties.withCredentials(username, password));
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        if (iface.isAssignableFrom(getClass())) {
            return iface.cast(this);
        }
        throw new SQLException("Cannot unwrap to " + iface.getName());
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return iface.isAssignableFrom(getClass());
    }

    @Override
    public PrintWriter getLogWriter() throws SQLException {
        return printWriter;
    }

    @Override
    public void setLogWriter(PrintWriter out) throws SQLException {
        this.printWriter = out;
    }

    @Override
    public void setLoginTimeout(int seconds) throws SQLException {
        this.loginTimeoutSeconds = seconds;
    }

    @Override
    public int getLoginTimeout() throws SQLException {
        return loginTimeoutSeconds;
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException();
    }
}
