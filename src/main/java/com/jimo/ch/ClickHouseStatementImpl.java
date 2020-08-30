package com.jimo.ch;

import com.google.common.base.Strings;
import com.jimo.ch.response.*;
import com.jimo.ch.setting.ClickHouseProperties;
import com.jimo.ch.setting.ClickHouseQueryParam;
import com.jimo.ch.util.ClickHouseRowBinaryInputStream;
import com.jimo.ch.util.Utils;
import com.jimo.ch.util.guava.StreamUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URISyntaxException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.util.*;

/**
 * @author jimo
 * @version 1.0.0
 * @date 2020/8/30 9:29
 */
public class ClickHouseStatementImpl implements ClickHouseStatement {

    private static final Logger log = LoggerFactory.getLogger(ClickHouseStatementImpl.class);

    private final CloseableHttpClient client;
    protected ClickHouseProperties properties;
    private ClickHouseConnection connection;
    private ClickHouseResultSet currentResult;
    private ClickHouseRowBinaryInputStream currentRowBinaryResult;

    private int currentUpdateCount = 1;
    private int queryTimeout;
    private boolean isQueryTimeoutSet = false;
    private int maxRows;
    private boolean closeOnCompletion;
    private final boolean isResultSetScrollable;
    private volatile String queryId;

    private final String initialDatabase;

    private static final String[] selectKeywords = new String[]{"SELECT", "WITH", "SHOW", "DESC", "EXISTS"};
    private static final String databaseKeyword = "CREATE DATABASE";

    public ClickHouseStatementImpl(CloseableHttpClient httpClient, ClickHouseConnection connection,
                                   ClickHouseProperties properties, int resultType) {
        this.client = httpClient;
        this.connection = connection;
        this.properties = properties == null ? new ClickHouseProperties() : properties;
        this.initialDatabase = this.properties.getDatabase();
        this.isResultSetScrollable = (resultType != ResultSet.TYPE_FORWARD_ONLY);
    }

    @Override
    public ResultSet executeQuery(String sql) throws SQLException {
        return null;
    }

    @Override
    public int executeUpdate(String sql) throws SQLException {
        return 0;
    }

    @Override
    public void close() throws SQLException {

    }

    @Override
    public int getMaxFieldSize() throws SQLException {
        return 0;
    }

    @Override
    public void setMaxFieldSize(int max) throws SQLException {

    }

    @Override
    public int getMaxRows() throws SQLException {
        return 0;
    }

    @Override
    public void setMaxRows(int max) throws SQLException {

    }

    @Override
    public void setEscapeProcessing(boolean enable) throws SQLException {

    }

    @Override
    public int getQueryTimeout() throws SQLException {
        return 0;
    }

    @Override
    public void setQueryTimeout(int seconds) throws SQLException {

    }

    @Override
    public void cancel() throws SQLException {

    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        return null;
    }

    @Override
    public void clearWarnings() throws SQLException {

    }

    @Override
    public void setCursorName(String name) throws SQLException {

    }

    @Override
    public boolean execute(String sql) throws SQLException {
        return false;
    }

    @Override
    public ResultSet getResultSet() throws SQLException {
        return null;
    }

    @Override
    public int getUpdateCount() throws SQLException {
        return 0;
    }

    @Override
    public boolean getMoreResults() throws SQLException {
        return false;
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {

    }

    @Override
    public int getFetchDirection() throws SQLException {
        return 0;
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {

    }

    @Override
    public int getFetchSize() throws SQLException {
        return 0;
    }

    @Override
    public int getResultSetConcurrency() throws SQLException {
        return 0;
    }

    @Override
    public int getResultSetType() throws SQLException {
        return 0;
    }

    @Override
    public void addBatch(String sql) throws SQLException {

    }

    @Override
    public void clearBatch() throws SQLException {

    }

    @Override
    public int[] executeBatch() throws SQLException {
        return new int[0];
    }

    @Override
    public ClickHouseConnection getConnection() throws SQLException {
        return null;
    }

    @Override
    public boolean getMoreResults(int current) throws SQLException {
        return false;
    }

    @Override
    public ResultSet getGeneratedKeys() throws SQLException {
        return null;
    }

    @Override
    public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
        return 0;
    }

    @Override
    public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
        return 0;
    }

    @Override
    public int executeUpdate(String sql, String[] columnNames) throws SQLException {
        return 0;
    }

    @Override
    public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
        return false;
    }

    @Override
    public boolean execute(String sql, int[] columnIndexes) throws SQLException {
        return false;
    }

    @Override
    public boolean execute(String sql, String[] columnNames) throws SQLException {
        return false;
    }

    @Override
    public int getResultSetHoldability() throws SQLException {
        return 0;
    }

    @Override
    public boolean isClosed() throws SQLException {
        return false;
    }

    @Override
    public void setPoolable(boolean poolable) throws SQLException {

    }

    @Override
    public boolean isPoolable() throws SQLException {
        return false;
    }

    @Override
    public void closeOnCompletion() throws SQLException {

    }

    @Override
    public boolean isCloseOnCompletion() throws SQLException {
        return false;
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        return null;
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return false;
    }

    @Override
    public ClickHouseResponse executeQueryClickHouseResponse(String sql) throws SQLException {
        return null;
    }

    @Override
    public ClickHouseResponse executeQueryClickHouseResponse(String sql,
                                                             Map<ClickHouseQueryParam, String> additionalParams) throws SQLException {
        return null;
    }

    @Override
    public ClickHouseResponse executeQueryClickHouseResponse(String sql,
                                                             Map<ClickHouseQueryParam, String> additionalParams,
                                                             Map<String, String> additionalRequestParams) throws SQLException {
        return null;
    }

    @Override
    public ClickHouseRowBinaryInputStream executeQueryClickHouseRowBinaryStream(String sql) throws SQLException {
        return null;
    }

    @Override
    public ClickHouseRowBinaryInputStream executeQueryClickHouseRowBinaryStream(String sql, Map<ClickHouseQueryParam,
            String> additionalParams) throws SQLException {
        return null;
    }

    @Override
    public ClickHouseRowBinaryInputStream executeQueryClickHouseRowBinaryStream(String sql, Map<ClickHouseQueryParam,
            String> additionalParams, Map<String, String> additionalRequestParams) throws SQLException {
        return null;
    }

    @Override
    public ResultSet executeQuery(String sql, Map<ClickHouseQueryParam, String> additionalParams) throws SQLException {
        return null;
    }

    @Override
    public ResultSet executeQuery(String sql, Map<ClickHouseQueryParam, String> additionalParams,
                                  Map<String, String> additionalRequestParams) throws SQLException {
        if (additionalParams == null || additionalParams.isEmpty()) {
            additionalParams = new EnumMap<>(ClickHouseQueryParam.class);
        } else {
            additionalParams = new EnumMap<>(additionalParams);
        }
        additionalParams.put(ClickHouseQueryParam.EXTREMES, "0");

        InputStream is = getInputStream(sql, additionalParams, additionalRequestParams);

        try {
            if (isSelect(sql)) {
                currentUpdateCount = -1;
                currentResult = createResultSet(properties.isCompress() ? new ClickHouseLZ4Stream(is) : is,
                        properties.getBufferSize(), extractDBName(sql), extractTableName(sql), extractWithTotals(sql)
                        , this, getConnection().getTimezone(), properties);
                currentResult.setMaxRows(maxRows);
                return currentResult;
            } else {
                currentUpdateCount = 0;
                StreamUtils.close(is);
                return null;
            }
        } catch (Exception e) {
            StreamUtils.close(is);
            throw new RuntimeException(e);
        }
    }


    private InputStream getInputStream(String sql,
                                       Map<ClickHouseQueryParam, String> additionalParams,
                                       Map<String, String> additionalRequestParams) {
        sql = clickhousifySql(sql);
        log.debug("execute sql: {}", sql);

        additionalParams = addQueryIdTo(additionalParams == null ?
                new EnumMap<ClickHouseQueryParam, String>(ClickHouseQueryParam.class) : additionalParams);

        final boolean ignoreDatabase = sql.trim().regionMatches(true, 0, databaseKeyword, 0, databaseKeyword.length());
        URI uri = buildRequestUri(null, additionalParams, additionalRequestParams, ignoreDatabase);
        log.debug("request url: {}", uri);

        HttpEntity requestEntity = new StringEntity(sql, StreamUtils.UTF_8);
        requestEntity = applyRequestBodyCompression(requestEntity);

        HttpEntity entity = null;
        try {
            uri = followRedirected(uri);
            final HttpPost post = new HttpPost(uri);
            post.setEntity(requestEntity);

            HttpResponse response = client.execute(post);
            entity = response.getEntity();
            checkForErrorAndThrow(entity, response);

            InputStream is;
            if (entity.isStreaming()) {
                is = entity.getContent();
            } else {
                final FastByteArrayOutputStream baos = new FastByteArrayOutputStream();
                entity.writeTo(baos);
                is = baos.convertToInputStream();
            }
            return is;
        } catch (Exception e) {
            log.error("Error connection to: {},message: {}", properties, e.getMessage());
            EntityUtils.consumeQuietly(entity);
            throw new RuntimeException(e);
        }
    }

    private void checkForErrorAndThrow(HttpEntity entity, HttpResponse response) throws IOException {
        if (response.getStatusLine().getStatusCode() != HttpURLConnection.HTTP_OK) {
            InputStream messageStream = entity.getContent();
            byte[] bytes = StreamUtils.toByteArray(messageStream);
            if (properties.isCompress()) {
                try {
                    messageStream = new ClickHouseLZ4Stream(new ByteArrayInputStream(bytes));
                    bytes = StreamUtils.toByteArray(messageStream);
                } catch (IOException e) {
                    log.warn("error while read compress stream:{}", e.getMessage());
                }
            }
            EntityUtils.consumeQuietly(entity);
            String chMsg = new String(bytes, StreamUtils.UTF_8);
            throw new RuntimeException(chMsg);
        }
    }

    private URI followRedirected(URI uri) throws IOException, URISyntaxException {
        if (properties.isCheckForRedirects()) {
            int redirects = 0;
            while (redirects < properties.getMaxRedirects()) {
                final HttpGet httpGet = new HttpGet(uri);
                HttpResponse response = client.execute(httpGet);
                if (response.getStatusLine().getStatusCode() == 307) {
                    uri = new URI(response.getHeaders("Location")[0].getValue());
                    redirects++;
                    log.info("Redirected to : {}", uri.getHost());
                } else {
                    break;
                }
            }
        }
        return uri;
    }

    private HttpEntity applyRequestBodyCompression(HttpEntity entity) {
        if (properties.isDecompress()) {
            return new LZ4EntityWrapper(entity, properties.getMaxCompressBufferSize());
        }
        return entity;
    }

    private URI buildRequestUri(String sql, Map<ClickHouseQueryParam, String> additionalParams,
                                Map<String, String> additionalRequestParams, boolean ignoreDatabase) {
        try {
            List<NameValuePair> queryParams = getUrlQueryParams(sql, additionalParams, additionalRequestParams,
                    ignoreDatabase);
            return new URIBuilder()
                    .setScheme(properties.getSsl() ? "https" : "http")
                    .setHost(properties.getHost())
                    .setPort(properties.getPort())
                    .setPath(properties.getPath() == null || properties.getPath().isEmpty() ? "/" :
                            properties.getPath())
                    .build();
        } catch (URISyntaxException e) {
            log.error("Mail Formed URL:{}", e.getMessage());
            throw new IllegalStateException("Illegal config of db");
        }
    }

    private List<NameValuePair> getUrlQueryParams(String sql, Map<ClickHouseQueryParam, String> additionalParams,
                                                  Map<String, String> additionalRequestParams, boolean ignoreDatabase) {
        List<NameValuePair> result = new ArrayList<>();

        if (sql != null) {
            result.add(new BasicNameValuePair("query", sql));
        }

        Map<ClickHouseQueryParam, String> params = properties.buildQueryParams(true);
        if (!ignoreDatabase) {
            params.put(ClickHouseQueryParam.DATABASE, initialDatabase);
        }
        if (additionalParams != null && !additionalParams.isEmpty()) {
            params.putAll(additionalParams);
        }

        setStatementPropertiesToParams(params);

        for (Map.Entry<ClickHouseQueryParam, String> entry : params.entrySet()) {
            if (!Strings.isNullOrEmpty(entry.getValue())) {
                result.add(new BasicNameValuePair(entry.getKey().toString(), entry.getValue()));
            }
        }

        if (additionalRequestParams != null) {
            for (Map.Entry<String, String> entry : additionalRequestParams.entrySet()) {
                if (!Strings.isNullOrEmpty(entry.getValue())) {
                    result.add(new BasicNameValuePair(entry.getKey(), entry.getValue()));
                }
            }
        }
        return result;
    }

    private void setStatementPropertiesToParams(Map<ClickHouseQueryParam, String> params) {
        if (maxRows > 0) {
            params.put(ClickHouseQueryParam.MAX_RESULT_ROWS, String.valueOf(maxRows));
            params.put(ClickHouseQueryParam.RESULT_OVERFLOW_MODE, "break");
        }
        if (isQueryTimeoutSet) {
            params.put(ClickHouseQueryParam.MAX_EXECUTION_TIME, String.valueOf(queryTimeout));
        }
    }

    private Map<ClickHouseQueryParam, String> addQueryIdTo(Map<ClickHouseQueryParam, String> parameter) {
        if (this.queryId != null) {
            return parameter;
        }
        final String queryId = parameter.get(ClickHouseQueryParam.QUERY_ID);
        if (queryId == null) {
            this.queryId = UUID.randomUUID().toString();
            parameter.put(ClickHouseQueryParam.QUERY_ID, this.queryId);
        } else {
            this.queryId = queryId;
        }
        return parameter;
    }

    private String clickhousifySql(String sql) {
        // TODO
        return sql;
    }

    static boolean isSelect(String sql) {
        for (int i = 0; i < sql.length(); i++) {
            String nextTwo = sql.substring(i, Math.min(i + 2, sql.length()));
            if ("--".equals(nextTwo)) {
                i = Math.max(i, sql.indexOf('\n', i));
            } else if ("/*".equals(nextTwo)) {
                i = Math.max(i, sql.indexOf("*/", i));
            } else if (Character.isLetter(sql.charAt(i))) {
                final String trimmed = sql.substring(i, Math.min(sql.length(), Math.max(i, sql.indexOf(" ", i))));
                for (String keyword : selectKeywords) {
                    if (trimmed.regionMatches(true, 0, keyword, 0, keyword.length())) {
                        return true;
                    }
                }
                return false;
            }
        }
        return false;
    }

    private boolean extractWithTotals(String sql) {
        if (Utils.startsWithIgnoreCase(sql, "select")) {
            final String s = Utils.retainUnquoted(sql, '\'');
            return s.toLowerCase().contains(" with totals");
        }
        return false;
    }

    private String extractDBName(String sql) {
        String s = extractDbAndTableName(sql);
        return s.contains(".") ? s.substring(0, s.indexOf(".")) : properties.getDatabase();
    }

    private String extractTableName(String sql) {
        String s = extractDbAndTableName(sql);
        return s.contains(".") ? s.substring(s.indexOf(".") + 1) : s;
    }

    private String extractDbAndTableName(String sql) {
        if (Utils.startsWithIgnoreCase(sql, "select")) {
            final String withoutStrings = Utils.retainUnquoted(sql, '\'');
            int fromIndex = withoutStrings.indexOf("from");
            if (fromIndex == -1) {
                fromIndex = withoutStrings.indexOf("FROM");
            }
            if (fromIndex != -1) {
                final String fromFrom = withoutStrings.substring(fromIndex);
                final String fromTable = fromFrom.substring("from".length()).trim();
                return fromTable.split(" ")[0];
            }
        }
        if (Utils.startsWithIgnoreCase(sql, "desc")) {
            return "system.columns";
        }
        if (Utils.startsWithIgnoreCase(sql, "show")) {
            return "system.tables";
        }
        return "system.unknown";
    }

    private ClickHouseResultSet createResultSet(InputStream is, int bufferSize, String db, String table,
                                                boolean useWithTotals,
                                                ClickHouseStatement statement, TimeZone timeZone,
                                                ClickHouseProperties properties) {
        if (isResultSetScrollable) {
            return new ClickHouseScrollableResultSet(is, bufferSize, db, table, useWithTotals, statement, timeZone,
                    properties);
        } else {
            return new ClickHouseResultSet(is, bufferSize, db, table, useWithTotals, statement, timeZone, properties);
        }
    }
}
