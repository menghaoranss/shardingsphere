/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.sphereex.dbplusengine.driver;

import com.sphereex.dbplusengine.driver.jdbc.core.driver.ConnectionPropertiesAwareDriverDataSourceCache;
import org.apache.shardingsphere.driver.exception.DriverRegisterException;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.util.Properties;
import java.util.logging.Logger;

/**
 * SphereEx driver.
 */
public final class SphereExDriver implements Driver {
    
    private static final String DRIVER_URL_PREFIX = "jdbc:sphereex:";
    
    private static final int MAJOR_DRIVER_VERSION = 1;
    
    private static final int MINOR_DRIVER_VERSION = 7;

    private final ConnectionPropertiesAwareDriverDataSourceCache dataSourceCache = new ConnectionPropertiesAwareDriverDataSourceCache();
    
    static {
        try {
            DriverManager.registerDriver(new SphereExDriver());
        } catch (final SQLException ex) {
            throw new DriverRegisterException(ex);
        }
    }
    
    @Override
    public Connection connect(final String url, final Properties info) throws SQLException {
        return acceptsURL(url) ? dataSourceCache.get(url, DRIVER_URL_PREFIX, info).getConnection() : null;
    }
    
    @Override
    public boolean acceptsURL(final String url) {
        return null != url && url.startsWith(DRIVER_URL_PREFIX);
    }
    
    @Override
    public DriverPropertyInfo[] getPropertyInfo(final String url, final Properties info) {
        return new DriverPropertyInfo[0];
    }
    
    @Override
    public int getMajorVersion() {
        return MAJOR_DRIVER_VERSION;
    }
    
    @Override
    public int getMinorVersion() {
        return MINOR_DRIVER_VERSION;
    }
    
    @Override
    public boolean jdbcCompliant() {
        return false;
    }
    
    @Override
    public Logger getParentLogger() {
        return Logger.getLogger(Logger.GLOBAL_LOGGER_NAME);
    }
}
