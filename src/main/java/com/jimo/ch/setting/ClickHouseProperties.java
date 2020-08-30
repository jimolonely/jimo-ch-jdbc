package com.jimo.ch.setting;

import java.util.Properties;

public class ClickHouseProperties {

    private String useTimeZone;
    private boolean useServerTimeZone;

    private int port;

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
}
