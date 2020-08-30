package com.jimo.ch;

import com.google.common.base.Strings;
import com.jimo.ch.exp.ClickHouseUnknownException;
import com.jimo.ch.setting.ClickHouseConnectionSettings;
import com.jimo.ch.setting.ClickHouseProperties;
import com.jimo.ch.util.ClickHouseHttpClientBuilder;
import com.jimo.ch.util.LogProxy;
import com.jimo.ch.util.guava.StreamUtils;
import org.apache.http.conn.ConnectTimeoutException;
import org.apache.http.impl.client.CloseableHttpClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.sql.*;
import java.util.Map;
import java.util.Properties;
import java.util.TimeZone;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

/**
 * @author jimo
 * @version 1.0.0
 * @date 2020/8/29 16:30
 */
public class ClickHouseConnectionImpl implements ClickHouseConnection {

    private static final int DEFAULT_RESULT_SET_TYPE = ResultSet.TYPE_FORWARD_ONLY;

    private static final Logger log = LoggerFactory.getLogger(ClickHouseConnectionImpl.class);

    private final CloseableHttpClient httpClient;
    private final ClickHouseProperties properties;
    private String url;
    private boolean closed = false;

    private TimeZone timeZone;
    private volatile String serverVersion;

    public ClickHouseConnectionImpl(String url, ClickHouseProperties info) {
        this.url = url;
        try {
            this.properties = ClickHouseJdbcUrlParser.parse(url, info.asProperties());
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException(e);
        }
        final ClickHouseHttpClientBuilder clientBuilder = new ClickHouseHttpClientBuilder(this.properties);
        log.debug("Create a new connection to {}", url);
        try {
            httpClient = clientBuilder.buildClient();
        } catch (Exception e) {
            throw new IllegalStateException("cannot initialize http client", e);
        }
        initTimeZone(this.properties);
    }

    private void initTimeZone(ClickHouseProperties properties) {
        if (properties.isUseServerTimeZone() && !Strings.isNullOrEmpty(properties.getUseTimeZone())) {
            throw new IllegalArgumentException(String.format("only one of %s or %s must be enabled",
                    ClickHouseConnectionSettings.USE_SERVER_TIME_ZONE.getKey(),
                    ClickHouseConnectionSettings.USE_TIME_ZONE.getKey()));
        }
        if (!properties.isUseServerTimeZone() && Strings.isNullOrEmpty(properties.getUseTimeZone())) {
            throw new IllegalArgumentException(String.format("one of %s or %s must be enabled",
                    ClickHouseConnectionSettings.USE_SERVER_TIME_ZONE.getKey(),
                    ClickHouseConnectionSettings.USE_TIME_ZONE.getKey()));
        }
        if (properties.isUseServerTimeZone()) {
            ResultSet rs = null;
            try {
                timeZone = TimeZone.getTimeZone("UTC");
                //noinspection SqlResolve
                rs = createStatement().executeQuery("select timezone()");
                rs.next();
                String timeZoneName = rs.getString(1);
                timeZone = TimeZone.getTimeZone(timeZoneName);
            } catch (SQLException e) {
                throw new RuntimeException(e);
            } finally {
                StreamUtils.close(rs);
            }
        } else if (!Strings.isNullOrEmpty(properties.getUseTimeZone())) {
            timeZone = TimeZone.getTimeZone(properties.getUseTimeZone());
        }
    }

    @Override
    public ClickHouseStatement createStatement() throws SQLException {
        return createStatement(DEFAULT_RESULT_SET_TYPE);
    }

    public ClickHouseStatement createStatement(int resultSetType) {
        return LogProxy.wrap(ClickHouseStatement.class, new ClickHouseStatementImpl(httpClient, this, properties,
                resultSetType));
    }

    public PreparedStatement createPreparedStatement(String sql, int resultType) throws SQLException {
        return LogProxy.wrap(PreparedStatement.class, new ClickHousePreparedStatementImpl(httpClient, this,
                properties, sql, getTimezone(), resultType));
    }

    @Override
    public PreparedStatement prepareStatement(String sql) throws SQLException {
        return createPreparedStatement(sql, DEFAULT_RESULT_SET_TYPE);
    }

    @Override
    public CallableStatement prepareCall(String sql) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public String nativeSQL(String sql) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setAutoCommit(boolean autoCommit) throws SQLException {
    }

    @Override
    public boolean getAutoCommit() throws SQLException {
        return false;
    }

    @Override
    public void commit() throws SQLException {
    }

    @Override
    public void rollback() throws SQLException {
    }

    @Override
    public void close() throws SQLException {
        try {
            httpClient.close();
            closed = true;
        } catch (IOException e) {
            throw new ClickHouseUnknownException("HTTP Client close exception", e, properties.getHost(),
                    properties.getPort());
        }
    }

    @Override
    public boolean isClosed() throws SQLException {
        return closed;
    }

    @Override
    public DatabaseMetaData getMetaData() throws SQLException {
        return LogProxy.wrap(DatabaseMetaData.class, new ClickHouseDatabaseMetaData(url, this));
    }

    @Override
    public void setReadOnly(boolean readOnly) throws SQLException {
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        return false;
    }

    @Override
    public void setCatalog(String catalog) throws SQLException {
        properties.setDatabase(catalog);
        final URI old = URI.create(url.substring(ClickHouseJdbcUrlParser.JDBC_PREFIX.length()));
        try {
            url = ClickHouseJdbcUrlParser.JDBC_PREFIX +
                    new URI(old.getScheme(), old.getUserInfo(), old.getHost(), old.getPort(), "/" + catalog,
                            old.getQuery(), old.getFragment());
        } catch (URISyntaxException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public String getCatalog() throws SQLException {
        return properties.getDatabase();
    }

    @Override
    public void setTransactionIsolation(int level) throws SQLException {
    }

    @Override
    public int getTransactionIsolation() throws SQLException {
        return Connection.TRANSACTION_NONE;
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        return null;
    }

    @Override
    public void clearWarnings() throws SQLException {
    }

    @Override
    public ClickHouseStatement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
        return createStatement(resultSetType, resultSetConcurrency, ResultSet.CLOSE_CURSORS_AT_COMMIT);
    }

    @Override
    public TimeZone getTimezone() {
        return timeZone;
    }

    @Override
    public String getServerVersion() throws SQLException {
        if (serverVersion == null) {
            final ResultSet rs = createStatement().executeQuery("SELECT version()");
            rs.next();
            serverVersion = rs.getString(1);
            rs.close();
        }
        return serverVersion;
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        return createPreparedStatement(sql, resultSetType);
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Map<String, Class<?>> getTypeMap() throws SQLException {
        return null;
    }

    @Override
    public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
    }

    @Override
    public void setHoldability(int holdability) throws SQLException {
    }

    @Override
    public int getHoldability() throws SQLException {
        return 0;
    }

    @Override
    public Savepoint setSavepoint() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Savepoint setSavepoint(String name) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void rollback(Savepoint savepoint) throws SQLException {
    }

    @Override
    public void releaseSavepoint(Savepoint savepoint) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public ClickHouseStatement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        if (resultSetType == ResultSet.TYPE_SCROLL_SENSITIVE || resultSetConcurrency != ResultSet.CONCUR_READ_ONLY
                || resultSetHoldability != ResultSet.CLOSE_CURSORS_AT_COMMIT) {
            throw new SQLFeatureNotSupportedException();
        }
        return createStatement(resultSetType);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency,
                                              int resultSetHoldability) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency,
                                         int resultSetHoldability) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
        return null;
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Clob createClob() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Blob createBlob() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public NClob createNClob() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public SQLXML createSQLXML() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean isValid(int timeout) throws SQLException {
        if (timeout < 0) {
            throw new SQLException("timeout value must not be less 0");
        }
        if (isClosed()) {
            return false;
        }

        boolean isAnotherHttpClient = false;
        CloseableHttpClient closeableHttpClient = null;
        try {
            if (timeout == 0) {
                closeableHttpClient = this.httpClient;
            } else {
                final ClickHouseProperties properties = new ClickHouseProperties(this.properties);
                properties.setConnectionTimout((int) TimeUnit.SECONDS.toMillis(timeout));
                properties.setMaxExecutionTime(timeout);
                closeableHttpClient = new ClickHouseHttpClientBuilder(properties).buildClient();
                isAnotherHttpClient = true;
            }
            Statement statement = createClickHouseStatement(closeableHttpClient);
            statement.execute("SELECT 1");
            statement.close();
            return true;
        } catch (Exception e) {
            boolean isFailOnConnectionTimeout = e.getCause() instanceof ConnectTimeoutException;
            if (!isFailOnConnectionTimeout) {
                log.warn("Something had happened while validating a connection", e);
            }
            return false;
        } finally {
            if (isAnotherHttpClient) {
                try {
                    closeableHttpClient.close();
                } catch (IOException e) {
                    log.warn("cannot close a http client", e);
                }
            }
        }
    }

    private ClickHouseStatement createClickHouseStatement(CloseableHttpClient httpClient) {
        return LogProxy.wrap(ClickHouseStatement.class, new ClickHouseStatementImpl(httpClient, this, properties,
                DEFAULT_RESULT_SET_TYPE));
    }

    @Override
    public void setClientInfo(String name, String value) throws SQLClientInfoException {

    }

    @Override
    public void setClientInfo(Properties properties) throws SQLClientInfoException {

    }

    @Override
    public String getClientInfo(String name) throws SQLException {
        return null;
    }

    @Override
    public Properties getClientInfo() throws SQLException {
        return null;
    }

    @Override
    public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
        // TODO
        return null;
    }

    @Override
    public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
        return null;
    }

    @Override
    public void setSchema(String schema) throws SQLException {
        properties.setDatabase(schema);
    }

    @Override
    public String getSchema() throws SQLException {
        return properties.getDatabase();
    }

    @Override
    public void abort(Executor executor) throws SQLException {
        this.close();
    }

    @Override
    public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {

    }

    @Override
    public int getNetworkTimeout() throws SQLException {
        return 0;
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        if (iface.isAssignableFrom(getClass())) {
            return iface.cast(this);
        }
        throw new SQLException("cannot unwrap to: " + iface.getName());
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return iface.isAssignableFrom(getClass());
    }
}
