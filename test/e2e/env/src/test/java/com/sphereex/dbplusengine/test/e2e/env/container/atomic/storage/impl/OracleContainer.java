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
import org.apache.shardingsphere.infra.database.core.type.DatabaseType;
import org.apache.shardingsphere.infra.database.oracle.type.OracleDatabaseType;
import org.apache.shardingsphere.infra.spi.type.typed.TypedSPILoader;
import org.apache.shardingsphere.test.e2e.env.container.atomic.constants.StorageContainerConstants;
import org.apache.shardingsphere.test.e2e.env.container.atomic.storage.DockerStorageContainer;
import org.apache.shardingsphere.test.e2e.env.container.atomic.storage.config.StorageContainerConfiguration;
import org.apache.shardingsphere.test.e2e.env.container.atomic.util.StorageContainerUtils;

import javax.sql.DataSource;
import java.time.Duration;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Oracle container.
 */
@SphereEx
public final class OracleContainer extends DockerStorageContainer {
    
    private final StorageContainerConfiguration storageContainerConfig;
    
    public OracleContainer(final String containerImage, final StorageContainerConfiguration storageContainerConfig) {
        super(TypedSPILoader.getService(DatabaseType.class, "Oracle"), Strings.isNullOrEmpty(containerImage) ? "quay.io/maksymbilenko/oracle-xe-11g" : containerImage);
        this.storageContainerConfig = storageContainerConfig;
    }
    
    @Override
    protected void configure() {
        setCommands(storageContainerConfig.getContainerCommand());
        addEnvs(storageContainerConfig.getContainerEnvironments());
        // SPEX CHANGED: BEGIN
        mountConfigurationFiles(storageContainerConfig.getMountedResources());
        // SPEX CHANGED: END
        super.configure();
        withStartupTimeout(Duration.ofMinutes(5));
    }
    
    @Override
    protected Collection<String> getDatabaseNames() {
        return storageContainerConfig.getDatabaseTypes().entrySet().stream()
                .filter(entry -> entry.getValue() instanceof OracleDatabaseType).map(Map.Entry::getKey).collect(Collectors.toList());
    }
    
    @Override
    protected Collection<String> getExpectedDatabaseNames() {
        return storageContainerConfig.getExpectedDatabaseTypes().entrySet().stream()
                .filter(entry -> entry.getValue() instanceof OracleDatabaseType).map(Map.Entry::getKey).collect(Collectors.toList());
    }
    
    @Override
    public int getExposedPort() {
        return StorageContainerConstants.ORACLE_EXPOSED_PORT;
    }
    
    @Override
    public int getMappedPort() {
        return getMappedPort(StorageContainerConstants.ORACLE_EXPOSED_PORT);
    }
    
    @Override
    protected Optional<String> getDefaultDatabaseName() {
        return Optional.of("XE");
    }
    
    @Override
    public DataSource createAccessDataSource(final String dataSourceName) {
        return StorageContainerUtils.generateDataSource(getJdbcUrl(dataSourceName), dataSourceName, dataSourceName, 4);
    }
}
