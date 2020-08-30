package com.jimo.ch.response;

import com.jimo.ch.ClickHouseStatement;
import com.jimo.ch.setting.ClickHouseProperties;

import java.io.IOException;
import java.io.InputStream;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.TimeZone;

/**
 * @author jimo
 * @version 1.0.0
 * @date 2020/8/30 10:41
 */
public class ClickHouseResultSet extends AbstractResultSet {

    private final static long[] EMPTY_LONG_ARRAY = new long[0];
    private static final String DATE_TIME_PATTERN = "yyyy-MM-dd HH:mm:ss";
    private static final String DATE_PATTERN = "yyyy-MM-dd";

    private final TimeZone datetimeTimeZone;
    private final TimeZone dateTimeZone;
    private final SimpleDateFormat dateTimeFormat = new SimpleDateFormat(DATE_TIME_PATTERN);
    private final SimpleDateFormat dateFormat = new SimpleDateFormat(DATE_PATTERN);

    private final StreamSplitter bis;

    private final String db;
    private final String table;

    private List<ClickHouseColumnInfo> columns;

    private int maxRows;

    private ByteFragment[] values;
    // 1-based
    private int lastReadColumn;

    protected ByteFragment nextLine;

    private ByteFragment totalLine;

    protected int rowNumber;

    private final ClickHouseStatement statement;

    private final ClickHouseProperties properties;

    private boolean useWithTotals;

    private boolean lastReached = false;

    private boolean isAfterLastReached = false;

    public ClickHouseResultSet(InputStream is, int bufferSize, String db, String table, boolean useWithTotals,
                               ClickHouseStatement statement, TimeZone timeZone, ClickHouseProperties properties) throws IOException {
        this.db = db;
        this.table = table;
        this.statement = statement;
        this.properties = properties;
        this.useWithTotals = useWithTotals;
        this.datetimeTimeZone = timeZone;
        this.dateTimeZone = properties.isUseServerTimeZoneForDates() ? timeZone : TimeZone.getDefault();
        dateTimeFormat.setTimeZone(datetimeTimeZone);
        dateFormat.setTimeZone(dateTimeZone);
        bis = new StreamSplitter(is, (byte) 0x0A, bufferSize); /// \n
        ByteFragment headerFragment = bis.next();
        if (headerFragment == null) {
            throw new IllegalArgumentException("response without column names");
        }
        String header = headerFragment.asString(true);
        if (header.startsWith("Code: ") && !header.contains("\t")) {
            is.close();
            throw new IOException("error: " + header);
        }
        String[] cols = toStringArray(headerFragment);
        ByteFragment typeFragment = bis.next();
        if (typeFragment == null) {
            throw new IllegalArgumentException("response without column types");
        }
        String[] types = toStringArray(typeFragment);
        columns = new ArrayList<>(cols.length);
        for (int i = 0; i < cols.length; i++) {
            columns.add(ClickHouseColumnInfo.parse(types[i], cols[i]));
        }
    }

    private String[] toStringArray(ByteFragment fragment) {
        ByteFragment[] split = fragment.split((byte) 0x09);
        final String[] c = new String[split.length];
        for (int i = 0; i < split.length; i++) {
            c[i] = split[i].asString(true);
        }
        return c;
    }

    public void setMaxRows(int maxRows) {

    }

    public boolean hasNext() throws SQLException {
        if (nextLine == null && !lastReached) {
            try {
                nextLine = bis.next();
                if (nextLine == null || (maxRows != 0 && rowNumber >= maxRows) || (useWithTotals && nextLine.length() == 0)) {
                    if (useWithTotals) {
                        if (onTheSeparatorRow()) {
                            totalLine = bis.next();
                            endOfStream();
                        } // or do not close the stream
                    } else {
                        endOfStream();
                    }
                }
            } catch (IOException e) {
                throw new SQLException(e);
            }
        }
        return nextLine != null;
    }

    private void endOfStream() throws IOException {
        bis.close();
        lastReached = true;
        nextLine = null;
    }

    private boolean onTheSeparatorRow() throws IOException {
        bis.mark();
        boolean onSeparatorRow = bis.next() != null && bis.next() == null;
        bis.reset();
        return onSeparatorRow;
    }

    @Override
    public boolean isAfterLast() throws SQLException {
        return isAfterLastReached;
    }

    @Override
    public boolean isFirst() throws SQLException {
        return rowNumber == 1;
    }

    @Override
    public boolean isLast() throws SQLException {
        return !hasNext();
    }

    @Override
    public boolean next() throws SQLException {
        if (hasNext()) {
            values = nextLine.split((byte) 0x09);
            checkValues(columns, values, nextLine);
            nextLine = null;
            rowNumber += 1;
            return true;
        }
        isAfterLastReached = true;
        return false;
    }

    private void checkValues(List<ClickHouseColumnInfo> columns, ByteFragment[] values, ByteFragment nextLine) throws SQLException {
        if (columns.size() != values.length) {
            throw new SQLException(nextLine.asString());
        }
    }

    @Override
    public void close() throws SQLException {
        try {
            bis.close();
        } catch (IOException e) {
            throw new SQLException(e);
        }
    }

    @Override
    public boolean isClosed() throws SQLException {
        return bis.isClose();
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        return new ClickHouseResultSetMetaData(this);
    }

    @Override
    public boolean wasNull() throws SQLException {
        if (lastReadColumn == 0) {
            throw new IllegalStateException("You should get something before check nullability");
        }
        return getValue(lastReadColumn).isNull();
    }

    private ByteFragment getValue(int colNum) {
        lastReadColumn = colNum;
        return values[colNum - 1];
    }

    @Override
    public int getInt(int columnIndex) throws SQLException {
        return ByteFragmentUtils.parseInt(getValue(columnIndex));
    }

    @Override
    public boolean getBoolean(int columnIndex) throws SQLException {
        return toBoolean(getValue(columnIndex));
    }

    private boolean toBoolean(ByteFragment value) {
        if (value.isNull()) {
            return false;
        }
        return "1".equals(value.asString());
    }

    @Override
    public long getLong(int columnIndex) throws SQLException {
        // TODO
        return 0;
    }

    @Override
    public byte[] getBytes(int columnIndex) throws SQLException {
        return toBytes(getValue(columnIndex));
    }

    private byte[] toBytes(ByteFragment value) {
        if (value.isNull()) {
            return null;
        }
        return value.unescape();
    }

    @Override
    public String getString(int columnIndex) throws SQLException {
        return toString(getValue(columnIndex));
    }

    private String toString(ByteFragment value) {
        return value.asString(true);
    }
}
