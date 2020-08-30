package com.jimo.ch;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.TimeZone;

public interface ClickHouseConnection extends Connection {

    @Override
    ClickHouseStatement createStatement() throws SQLException;

    @Override
    ClickHouseStatement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException;

    TimeZone getTimezone();

    String getServerVersion() throws SQLException;
}
