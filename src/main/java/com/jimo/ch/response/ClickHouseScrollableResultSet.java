package com.jimo.ch.response;

import com.jimo.ch.ClickHouseStatement;
import com.jimo.ch.setting.ClickHouseProperties;

import java.io.InputStream;
import java.util.TimeZone;

/**
 * @author jimo
 * @version 1.0.0
 * @date 2020/8/30 13:01
 */
public class ClickHouseScrollableResultSet extends ClickHouseResultSet {

    public ClickHouseScrollableResultSet(InputStream is, int bufferSize, String db, String table,
                                         boolean useWithTotals, ClickHouseStatement statement, TimeZone timeZone,
                                         ClickHouseProperties properties) {
        super(is, bufferSize, db, table, useWithTotals, statement, timeZone, properties);
    }
}
