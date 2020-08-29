package com.jimo.ch.setting;

import java.sql.DriverPropertyInfo;
import java.util.Properties;

/**
 * @author jimo
 * @version 1.0.0
 * @date 2020/8/29 17:04
 */
public interface DriverPropertyCreator {

    DriverPropertyInfo createDriverPropertyInfo(Properties properties);
}
