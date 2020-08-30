package com.jimo.ch.setting;

import java.util.Map;
import java.util.Properties;

public class ClickHouseProperties {

    private String useTimeZone;
    private boolean useServerTimeZone;

    private int port;
    /**
     * If set to <code>true</code>, driver will first try to connect to the server using GET request. If the response
     * is 307,
     * it will use URI given in the response's Location header instead of the original one.
     * <p>
     * Those queries will be repeated until response is anything other than 307, or until
     * {@link ClickHouseProperties#maxRedirects maxRedirects} is hit.
     * <p>
     * This is a workaround to issues with properly following HTTP POST redirects.
     * Namely, Apache HTTP client's inability to process early responses, and difficulties with resending non-repeatable
     * {@link org.apache.http.entity.InputStreamEntity InputStreamEntity}
     */
    private boolean checkForRedirects;
    private int maxRedirects;

    public ClickHouseProperties(Properties info) {

    }

    public ClickHouseProperties(ClickHouseProperties clickHouseProperties) {

    }

    public ClickHouseProperties() {

    }

    public Properties asProperties() {
        // TODO
        return new Properties();
    }

    public ClickHouseProperties withCredentials(String username, String password) {
        final ClickHouseProperties copy = new ClickHouseProperties(this);
        // TODO
        return copy;
    }

    public String getHost() {
        return null;
    }

    public boolean isUseServerTimeZone() {
        return useServerTimeZone;
    }

    public String getUseTimeZone() {
        return useTimeZone;
    }

    public int getPort() {
        return port;
    }

    public void setDatabase(String catalog) {

    }

    public String getDatabase() {
        return null;
    }

    public void setConnectionTimout(int timeout) {

    }

    public void setMaxExecutionTime(int max) {

    }

    public boolean isCompress() {
        return false;
    }

    public int getBufferSize() {
        return 0;
    }

    public boolean getSsl() {
        return false;
    }

    public String getPath() {
        return null;
    }

    public Map<ClickHouseQueryParam, String> buildQueryParams(boolean ignoreDatabase) {
        return null;
    }

    public boolean isDecompress() {
        return false;
    }

    public int getMaxCompressBufferSize() {
        return 0;
    }

    public boolean isCheckForRedirects() {
        return checkForRedirects;
    }

    public int getMaxRedirects() {
        return maxRedirects;
    }
}
