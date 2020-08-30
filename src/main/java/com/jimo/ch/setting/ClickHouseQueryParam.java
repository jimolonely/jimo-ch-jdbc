package com.jimo.ch.setting;

import java.sql.DriverPropertyInfo;
import java.util.Properties;

/**
 * @author jimo
 * @version 1.0.0
 * @date 2020/8/29 16:54
 */
public enum ClickHouseQueryParam implements DriverPropertyCreator {

    COMPRESS("compress", false, Boolean.class, "whether to compress transfer data or not"),
    QUERY_ID("query_id", null, String.class, ""),
    DATABASE("database", null, String.class, "database name used by default"),


    MAX_RESULT_ROWS("max_result_rows", null, Integer.class, "Limit on the number of rows in the result. Also checked " +
            "for subqueries, and on remote servers when running parts of a distributed query."),

    RESULT_OVERFLOW_MODE("result_overflow_mode", null, String.class, "What to do if the volume of the result exceeds " +
            "one of the limits: 'throw' or 'break'. By default, throw. Using 'break' is similar to using LIMIT."),
    /**
     * https://clickhouse.yandex/reference_en.html#max_execution_time
     */
    MAX_EXECUTION_TIME("max_execution_time", null, Integer.class, "Maximum query execution time in seconds."),

    /**
     * https://clickhouse.yandex/reference_en.html#Extreme values
     */
    EXTREMES("extremes", false, Boolean.class, "Whether to include extreme values.");

    private final String key;
    private final Object defaultValue;
    private final Class clazz;
    private final String desc;

    <T> ClickHouseQueryParam(String key, T defaultValue, Class<T> clazz, String desc) {
        this.key = key;
        this.defaultValue = defaultValue;
        this.clazz = clazz;
        this.desc = desc;
    }

    public String getKey() {
        return key;
    }

    public Object getDefaultValue() {
        return defaultValue;
    }

    public Class getClazz() {
        return clazz;
    }

    public String getDesc() {
        return desc;
    }

    @Override
    public String toString() {
        return name().toLowerCase();
    }

    @Override
    public DriverPropertyInfo createDriverPropertyInfo(Properties properties) {
        final DriverPropertyInfo propertyInfo = new DriverPropertyInfo(key, driverPropertyValue(properties));
        propertyInfo.required = false;
        propertyInfo.description = desc;
        propertyInfo.choices = driverPropertyInfoChoices();
        return propertyInfo;
    }

    private String driverPropertyValue(Properties properties) {
        String value = properties.getProperty(key);
        if (value == null) {
            value = defaultValue == null ? null : defaultValue.toString();
        }
        return value;
    }

    private String[] driverPropertyInfoChoices() {
        return clazz == Boolean.class || clazz == Boolean.TYPE ? new String[]{"true", "false"} : null;
    }
}
