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

import com.github.dockerjava.api.model.HealthCheck;
import org.apache.shardingsphere.infra.autogen.version.ShardingSphereVersion;
import org.apache.shardingsphere.infra.database.core.type.DatabaseType;
import org.apache.shardingsphere.infra.database.oracle.type.OracleDatabaseType;
import org.apache.shardingsphere.infra.spi.type.typed.TypedSPILoader;
import org.apache.shardingsphere.test.e2e.env.container.atomic.constants.StorageContainerConstants;
import org.apache.shardingsphere.test.e2e.env.container.atomic.storage.DockerStorageContainer;
import org.apache.shardingsphere.test.e2e.env.container.atomic.storage.config.StorageContainerConfiguration;
import org.apache.shardingsphere.test.e2e.env.container.atomic.util.StorageContainerUtils;
import org.apache.shardingsphere.test.e2e.env.container.wait.JdbcConnectionWaitStrategy;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.BindMode;

import javax.sql.DataSource;
import java.sql.DriverManager;
import java.time.Duration;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Official Oracle 23c Free container.
 */
public final class OfficialOracle23cFreeContainer extends DockerStorageContainer {
    
    private static final String IMAGE = "container-registry.oracle.com/database/free:23.2.0.0";
    
    private static final String READY_USER = "ready_user";
    
    private static final String READY_USER_PASSWORD = "Ready@123";
    
    private final StorageContainerConfiguration storageContainerConfig;
    
    public OfficialOracle23cFreeContainer(final StorageContainerConfiguration storageContainerConfig) {
        super(TypedSPILoader.getService(DatabaseType.class, "Oracle"), IMAGE);
        this.storageContainerConfig = storageContainerConfig;
    }
    
    @Override
    protected void configure() {
        withCreateContainerCmdModifier(c -> c.withHealthcheck(new HealthCheck().withInterval(TimeUnit.SECONDS.toNanos(5))));
        setCommands(storageContainerConfig.getContainerCommand());
        addEnvs(storageContainerConfig.getContainerEnvironments());
        mapResources(storageContainerConfig.getMountedResources());
        withClasspathResourceMapping("/container/init-sql/oracle/00-init-authority.sql", "/docker-entrypoint-initdb.d/startup/00-init-authority.sql", BindMode.READ_ONLY);
        withClasspathResourceMapping("/container/init-sql/oracle/99-be-ready.sql", "/docker-entrypoint-initdb.d/startup/99-be-ready.sql", BindMode.READ_ONLY);
        withClasspathResourceMapping("/sphereex-dbplusengine-udf-" + ShardingSphereVersion.VERSION, "/sphereex-dbplusengine-udf", BindMode.READ_ONLY);
        withExposedPorts(getExposedPort());
        setWaitStrategy(new JdbcConnectionWaitStrategy(
                () -> DriverManager.getConnection(String.format("jdbc:oracle:thin:@localhost:%s/%s", getFirstMappedPort(), getDefaultDatabaseName().orElse("")), READY_USER, READY_USER_PASSWORD))
                        .withStartupTimeout(Duration.ofMinutes(3)));
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
        return Optional.of("FREEPDB1");
    }
    
    @Override
    public DataSource createAccessDataSource(final String dataSourceName) {
        return StorageContainerUtils.generateDataSource(getJdbcUrl(dataSourceName), dataSourceName, dataSourceName, 4);
    }
    
    @Override
    public String getJdbcUrl(final String unused) {
        return String.format("jdbc:oracle:thin:@%s:%s/%s", getHost(), getFirstMappedPort(), getDefaultDatabaseName().orElse(""));
    }
    
    @Override
    public String getHost() {
        return DockerClientFactory.instance().client().inspectNetworkCmd().withNetworkId(getNetwork().getId()).exec().getIpam().getConfig().get(0).getGateway();
    }
}
