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

package com.sphereex.dbplusengine.driver.jdbc.core.driver;

import com.sphereex.dbplusengine.driver.api.yaml.YamlDirectoryDataSourceFactory;
import com.sphereex.dbplusengine.driver.api.yaml.YamlSphereExDataSourceFactory;
import lombok.Getter;
import org.apache.shardingsphere.infra.exception.core.ShardingSpherePreconditions;
import org.apache.shardingsphere.infra.url.core.ShardingSphereURL;
import org.apache.shardingsphere.infra.url.core.ShardingSphereURLLoadEngine;
import org.apache.shardingsphere.sharding.exception.metadata.MissingRequiredShardingConfigurationException;

import javax.sql.DataSource;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Connection properties aware driver data source cache.
 */
@Getter
public final class ConnectionPropertiesAwareDriverDataSourceCache {
    
    private final Map<String, DataSource> dataSourceMap = new ConcurrentHashMap<>();
    
    /**
     * Get data source.
     *
     * @param url URL
     * @param urlPrefix URL prefix
     * @param connectionProps connection properties
     * @return got data source
     */
    public DataSource get(final String url, final String urlPrefix, final Properties connectionProps) {
        if (dataSourceMap.containsKey(url)) {
            return dataSourceMap.get(url);
        }
        return dataSourceMap.computeIfAbsent(url, driverUrl -> createDataSource(ShardingSphereURL.parse(driverUrl.substring(urlPrefix.length())), connectionProps));
    }
    
    @SuppressWarnings("unchecked")
    private <T extends Throwable> DataSource createDataSource(final ShardingSphereURL url, final Properties connectionProps) throws T {
        try {
            ShardingSphereURLLoadEngine urlLoadEngine = new ShardingSphereURLLoadEngine(url);
            return urlLoadEngine.isDirectory()
                    ? YamlDirectoryDataSourceFactory.createDataSource(urlLoadEngine.loadContents(), connectionProps, getDatabaseParameter(url))
                    : YamlSphereExDataSourceFactory.createDataSource(urlLoadEngine.loadContent(), connectionProps);
        } catch (final IOException ex) {
            throw (T) new SQLException(ex);
        } catch (final SQLException ex) {
            throw (T) ex;
        }
    }
    
    private String getDatabaseParameter(final ShardingSphereURL url) {
        String result = url.getQueryProps().getProperty("databaseName");
        ShardingSpherePreconditions.checkNotNull(result, () -> new MissingRequiredShardingConfigurationException("Database name url parameter"));
        return result;
    }
}
