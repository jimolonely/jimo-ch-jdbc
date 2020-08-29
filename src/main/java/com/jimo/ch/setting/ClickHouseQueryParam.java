package com.jimo.ch.setting;

import java.sql.DriverPropertyInfo;
import java.util.Properties;

/**
 * @author jimo
 * @version 1.0.0
 * @date 2020/8/29 16:54
 */
public enum ClickHouseQueryParam implements DriverPropertyCreator {

    COMPRESS("compress", false, Boolean.class, "whether to compress transfer data or not");

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
