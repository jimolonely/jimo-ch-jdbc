package com.jimo.ch;


import com.jimo.ch.response.ClickHouseResponse;
import com.jimo.ch.setting.ClickHouseQueryParam;
import com.jimo.ch.util.ClickHouseRowBinaryInputStream;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;

public interface ClickHouseStatement extends Statement {

    ClickHouseResponse executeQueryClickHouseResponse(String sql) throws SQLException;

    ClickHouseResponse executeQueryClickHouseResponse(String sql, Map<ClickHouseQueryParam, String> additionalParams) throws SQLException;

    ClickHouseResponse executeQueryClickHouseResponse(String sql, Map<ClickHouseQueryParam, String> additionalParams,
                                                      Map<String, String> additionalRequestParams) throws SQLException;

    ClickHouseRowBinaryInputStream executeQueryClickHouseRowBinaryStream(String sql) throws SQLException;

    ClickHouseRowBinaryInputStream executeQueryClickHouseRowBinaryStream(String sql, Map<ClickHouseQueryParam,
            String> additionalParams) throws SQLException;

    ClickHouseRowBinaryInputStream executeQueryClickHouseRowBinaryStream(String sql, Map<ClickHouseQueryParam,
            String> additionalParams, Map<String, String> additionalRequestParams) throws SQLException;

    ResultSet executeQuery(String sql, Map<ClickHouseQueryParam, String> additionalParams) throws SQLException;

    ResultSet executeQuery(String sql, Map<ClickHouseQueryParam, String> additionalParams,
                           Map<String, String> additionalRequestParams) throws SQLException;

}
