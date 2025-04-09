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

package com.sphereex.dbplusengine.test.e2e.env.container.atomic.storage.impl;

import com.google.common.base.Strings;
import com.sphereex.dbplusengine.SphereEx;
import com.sphereex.dbplusengine.infra.util.DatabaseTypeUtils;
import com.zaxxer.hikari.HikariDataSource;
import lombok.Getter;
import lombok.Setter;
import org.apache.shardingsphere.infra.database.core.type.DatabaseType;
import org.apache.shardingsphere.test.e2e.env.container.atomic.constants.StorageContainerConstants;
import org.apache.shardingsphere.test.e2e.env.container.atomic.storage.StorageContainer;
import org.apache.shardingsphere.test.e2e.env.container.atomic.storage.config.StorageContainerConfiguration;
import org.apache.shardingsphere.test.e2e.env.container.atomic.storage.config.impl.StorageContainerConfigurationFactory;
import org.apache.shardingsphere.test.e2e.env.container.atomic.util.BatchedSQLUtils;
import org.apache.shardingsphere.test.e2e.env.container.atomic.util.StorageContainerUtils;
import org.apache.shardingsphere.test.e2e.env.runtime.DataSourceEnvironment;
import org.apache.shardingsphere.test.e2e.env.runtime.E2ETestEnvironment;

import javax.sql.DataSource;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Native storage container.
 */
@SphereEx
@Getter
public final class NativeStorageContainer implements StorageContainer {
    
    private final DatabaseType databaseType;
    
    private final String scenario;
    
    private final Map<String, DataSource> actualDataSourceMap;
    
    private final Map<String, DataSource> expectedDataSourceMap;
    
    private final StorageContainerConfiguration storageContainerConfiguration;
    
    @Setter
    private List<String> networkAliases;
    
    public NativeStorageContainer(final DatabaseType databaseType, final String scenario) {
        this.databaseType = databaseType;
        this.scenario = scenario;
        storageContainerConfiguration = StorageContainerConfigurationFactory.newInstance(this.databaseType, this.scenario);
        initDatabase();
        actualDataSourceMap = createActualDataSourceMap();
        expectedDataSourceMap = createExpectedDataSourceMap();
    }
    
    private void initDatabase() {
        DataSource dataSource = StorageContainerUtils.generateDataSource(
                DataSourceEnvironment.getURL(databaseType, E2ETestEnvironment.getInstance().getNativeHost(), Integer.parseInt(E2ETestEnvironment.getInstance().getNativePort())),
                E2ETestEnvironment.getInstance().getNativeUsername(), E2ETestEnvironment.getInstance().getNativePassword());
        storageContainerConfiguration.getMountedResources().keySet().stream().filter(each -> each.toLowerCase()
                .endsWith(".sql")).forEach(each -> BatchedSQLUtils.execute(databaseType, scenario, dataSource, each));
    }
    
    private Map<String, DataSource> createActualDataSourceMap() {
        List<String> databaseNames =
                storageContainerConfiguration.getDatabaseTypes().entrySet().stream().filter(entry -> entry.getValue() == databaseType).map(Map.Entry::getKey).collect(Collectors.toList());
        return getDataSourceMap(databaseNames);
    }
    
    private Map<String, DataSource> createExpectedDataSourceMap() {
        List<String> databaseNames =
                storageContainerConfiguration.getExpectedDatabaseTypes().entrySet().stream().filter(entry -> entry.getValue() == databaseType).map(Map.Entry::getKey).collect(Collectors.toList());
        return getDataSourceMap(databaseNames);
    }
    
    private Map<String, DataSource> getDataSourceMap(final List<String> databaseNames) {
        Map<String, DataSource> result = new HashMap<>();
        for (String databaseName : databaseNames) {
            DataSource dataSource = StorageContainerUtils.generateDataSource(DataSourceEnvironment.getURL(databaseType, E2ETestEnvironment.getInstance().getNativeHost(),
                            Integer.parseInt(E2ETestEnvironment.getInstance().getNativePort()), databaseName),
                    E2ETestEnvironment.getInstance().getNativeUsername(), E2ETestEnvironment.getInstance().getNativePassword());
            initDialectDataSource(databaseName, dataSource);
            result.put(databaseName, dataSource);
        }
        return result;
    }
    
    private void initDialectDataSource(final String dataSourceName, final DataSource dataSource) {
        if (DatabaseTypeUtils.isOracleDatabase(databaseType)) {
            String connectionInitSql = "BEGIN\n";
            if (!Strings.isNullOrEmpty(dataSourceName)) {
                connectionInitSql += "EXECUTE IMMEDIATE 'ALTER SESSION SET CURRENT_SCHEMA=" + dataSourceName + "';\n";
            }
            connectionInitSql += "EXECUTE IMMEDIATE 'ALTER SESSION SET nls_date_language=american';\n";
            connectionInitSql += "END;";
            ((HikariDataSource) dataSource).setConnectionInitSql(connectionInitSql);
        }
    }
    
    @Override
    public String getAbbreviation() {
        return databaseType.getType().toLowerCase();
    }
    
    @Override
    public Map<String, String> getLinkReplacements() {
        Map<String, String> replacements = new HashMap<>();
        for (String each : getNetworkAliases()) {
            replacements.put(each + ":" + getExposedPort(), E2ETestEnvironment.getInstance().getNativeHost() + ":" + E2ETestEnvironment.getInstance().getNativePort());
        }
        replacements.put(StorageContainerConstants.USERNAME, E2ETestEnvironment.getInstance().getNativeUsername());
        replacements.put(StorageContainerConstants.PASSWORD, E2ETestEnvironment.getInstance().getNativePassword());
        return replacements;
    }
    
    private int getExposedPort() {
        if (databaseType.getType().equalsIgnoreCase("oracle")) {
            return 1521;
        } else if (databaseType.getType().equalsIgnoreCase("mysql")) {
            return 3306;
        } else if (databaseType.getType().equalsIgnoreCase("postgresql")) {
            return 5432;
        } else if (databaseType.getType().equalsIgnoreCase("OceanBase_Oracle")) {
            return 2883;
        } else {
            throw new UnsupportedOperationException("Unsupported database type: " + databaseType.getType());
        }
    }
    
    @Override
    public void start() {
    }
}
