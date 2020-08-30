package com.jimo.ch.response;

import com.jimo.ch.ClickHouseStatement;

import java.io.InputStream;
import java.util.TimeZone;

/**
 * @author jimo
 * @version 1.0.0
 * @date 2020/8/30 10:41
 */
public class ClickHouseResultSet extends AbstractResultSet {

    public ClickHouseResultSet(InputStream is, int bufferSize, String db, String table, boolean useWithTotals,
                               ClickHouseStatement statement, TimeZone timeZone, Object properties) {

    }

    public void setMaxRows(int maxRows) {

    }
}
