package com.jimo.ch.setting;

import java.util.Properties;

public class ClickHouseProperties {

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
}
