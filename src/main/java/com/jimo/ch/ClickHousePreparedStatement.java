package com.jimo.ch;

import com.jimo.ch.response.ClickHouseResponse;
import com.jimo.ch.setting.ClickHouseQueryParam;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

public interface ClickHousePreparedStatement extends PreparedStatement, ClickHouseStatement {
    ClickHouseResponse executeQueryClickHouseResponse() throws SQLException;

    void setArray(int parameterIndex, Object[] array) throws SQLException;

    ResultSet executeQuery(Map<ClickHouseQueryParam, String> additionParams) throws SQLException;

    int[] executeBatch(Map<ClickHouseQueryParam, String> additionalParams) throws SQLException;

    String asSql();
}
