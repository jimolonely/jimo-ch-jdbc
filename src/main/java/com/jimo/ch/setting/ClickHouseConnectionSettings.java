package com.jimo.ch.setting;

import java.sql.DriverPropertyInfo;
import java.util.Properties;

/**
 * @author jimo
 * @version 1.0.0
 * @date 2020/8/29 17:00
 */
public enum ClickHouseConnectionSettings implements DriverPropertyCreator {

    ASYNC("async", false, "");

    private final String key;
    private final Object defaultValue;
    private final String desc;
    private final Class clazz;

    ClickHouseConnectionSettings(String key, Object defaultValue, String desc) {
        this.key = key;
        this.defaultValue = defaultValue;
        this.desc = desc;
        this.clazz = defaultValue.getClass();
    }

    public String getKey() {
        return key;
    }

    public Object getDefaultValue() {
        return defaultValue;
    }

    public String getDesc() {
        return desc;
    }

    public Class getClazz() {
        return clazz;
    }

    @Override
    public DriverPropertyInfo createDriverPropertyInfo(Properties properties) {
        DriverPropertyInfo propertyInfo = new DriverPropertyInfo(key, driverPropertyValue(properties));
        propertyInfo.required = false;
        propertyInfo.description = desc;
        propertyInfo.choices = driverPropertyInfoChoices();
        return propertyInfo;
    }

    private String[] driverPropertyInfoChoices() {
        return clazz == Boolean.class || clazz == Boolean.TYPE ? new String[]{"true", "false"} : null;
    }

    private String driverPropertyValue(Properties properties) {
        String value = properties.getProperty(key);
        if (value == null) {
            value = defaultValue == null ? null : defaultValue.toString();
        }
        return value;
    }
}
