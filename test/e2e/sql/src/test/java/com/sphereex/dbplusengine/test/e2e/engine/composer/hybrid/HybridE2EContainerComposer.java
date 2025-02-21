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

package com.sphereex.dbplusengine.test.e2e.engine.composer.hybrid;

import com.sphereex.dbplusengine.infra.util.DatabaseTypeUtils;
import com.sphereex.dbplusengine.test.e2e.env.container.atomic.storage.config.impl.oracle.OfficialOracleContainerConfigurationFactory;
import com.sphereex.dbplusengine.test.e2e.env.container.atomic.storage.config.impl.oracle.Oracle11gEEContainerConfigurationFactory;
import com.sphereex.dbplusengine.test.e2e.env.container.atomic.storage.impl.OfficialOracle23cFreeContainer;
import com.sphereex.dbplusengine.test.e2e.env.container.atomic.storage.impl.Oracle11gEEContainer;
import com.zaxxer.hikari.HikariDataSource;
import lombok.Getter;
import org.apache.shardingsphere.infra.database.core.DefaultDatabase;
import org.apache.shardingsphere.infra.database.core.type.DatabaseType;
import org.apache.shardingsphere.infra.datasource.pool.destroyer.DataSourcePoolDestroyer;
import org.apache.shardingsphere.infra.spi.type.typed.TypedSPILoader;
import org.apache.shardingsphere.test.e2e.container.config.ProxyClusterContainerConfigurationFactory;
import org.apache.shardingsphere.test.e2e.env.container.atomic.ITContainers;
import org.apache.shardingsphere.test.e2e.env.container.atomic.adapter.impl.ShardingSphereJdbcContainer;
import org.apache.shardingsphere.test.e2e.env.container.atomic.adapter.impl.ShardingSphereProxyClusterContainer;
import org.apache.shardingsphere.test.e2e.env.container.atomic.enums.AdapterMode;
import org.apache.shardingsphere.test.e2e.env.container.atomic.governance.GovernanceContainer;
import org.apache.shardingsphere.test.e2e.env.container.atomic.governance.GovernanceContainerFactory;
import org.apache.shardingsphere.test.e2e.env.container.atomic.storage.DockerStorageContainer;
import org.apache.shardingsphere.test.e2e.env.container.atomic.storage.StorageContainer;
import org.apache.shardingsphere.test.e2e.env.container.atomic.util.AdapterContainerUtils;
import org.apache.shardingsphere.test.e2e.env.runtime.scenario.path.ScenarioCommonPath;
import org.apache.shardingsphere.test.e2e.env.runtime.scenario.path.ScenarioDataPath;
import org.h2.tools.RunScript;

import javax.sql.DataSource;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;

/**
 * Hybrid E2E container composer.
 */
@Getter
public final class HybridE2EContainerComposer {
    
    private static final String REGISTER_STORAGE_UNIT = "REGISTER STORAGE UNIT %s (URL='%s', USER='%s', PASSWORD='%s', PROPERTIES('minPoolSize'='%d', 'maxPoolSize'='%d'))";
    
    private final ITContainers containers;
    
    private final Map<String, DataSource> actualDataSourceMap;
    
    private final Map<String, DataSource> expectedDataSourceMap;
    
    private final GovernanceContainer governanceContainer;
    
    private final ShardingSphereProxyClusterContainer proxyContainer;
    
    private final ShardingSphereJdbcContainer driverContainer;
    
    private final DataSource proxyDataSource;
    
    private final String mode;
    
    HybridE2EContainerComposer(final String scenario, final DatabaseType databaseType, final String mode) throws SQLException, IOException {
        containers = new ITContainers(scenario);
        StorageContainer storageContainer = containers.registerContainer(createStorageContainer(scenario));
        actualDataSourceMap = storageContainer.getActualDataSourceMap();
        expectedDataSourceMap = storageContainer.getExpectedDataSourceMap();
        governanceContainer = containers.registerContainer(GovernanceContainerFactory.newInstance("ZooKeeper"));
        proxyContainer = containers.registerContainer(new ShardingSphereProxyClusterContainer(getProperFrontendDatabaseType(databaseType),
                ProxyClusterContainerConfigurationFactory.newInstance(scenario, databaseType, AdapterContainerUtils.getAdapterContainerImage())));
        driverContainer = containers.registerContainer(new ShardingSphereJdbcContainer(storageContainer, new ScenarioCommonPath(scenario).getRuleConfigurationFile(databaseType)));
        containers.start();
        proxyDataSource = proxyContainer.getTargetDataSource(governanceContainer.getServerLists());
        executeLogicDatabaseInit(scenario, databaseType, proxyDataSource);
        this.mode = mode;
    }
    
    private DockerStorageContainer createStorageContainer(final String scenario) {
        // TODO Support Oracle only for now
        switch (System.getProperty("plsql.e2e.database.version", "11")) {
            case "11":
                return new Oracle11gEEContainer(Oracle11gEEContainerConfigurationFactory.newInstance(scenario));
            case "23":
                return new OfficialOracle23cFreeContainer(OfficialOracleContainerConfigurationFactory.newInstance(scenario));
            default:
                throw new IllegalArgumentException("HybridE2EContainerComposer supports Oracle '11' or '23' only for now. Example: -Dplsql.e2e.database.version=11");
        }
    }
    
    private DatabaseType getProperFrontendDatabaseType(final DatabaseType databaseType) {
        return DatabaseTypeUtils.isOracleDatabase(databaseType) ? TypedSPILoader.getService(DatabaseType.class, "MySQL") : databaseType;
    }
    
    private void executeLogicDatabaseInit(final String scenario, final DatabaseType databaseType, final DataSource dataSource) throws SQLException, IOException {
        Optional<String> logicDatabaseInitSQLFile = new ScenarioDataPath(scenario).findActualDatabaseInitSQLFile(DefaultDatabase.LOGIC_NAME, databaseType);
        if (!logicDatabaseInitSQLFile.isPresent()) {
            return;
        }
        try (
                Connection connection = dataSource.getConnection();
                FileReader reader = new FileReader(logicDatabaseInitSQLFile.get())) {
            registerStorageUnitToProxy(connection);
            RunScript.execute(connection, reader);
        }
    }
    
    private void registerStorageUnitToProxy(final Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            for (Entry<String, DataSource> entry : actualDataSourceMap.entrySet()) {
                HikariDataSource dataSource = (HikariDataSource) entry.getValue();
                String registerStorageUnitDistSQL = String.format(REGISTER_STORAGE_UNIT, entry.getKey(), dataSource.getJdbcUrl(), dataSource.getUsername(), dataSource.getPassword(),
                        Math.max(dataSource.getMinimumIdle(), 0), dataSource.getMaximumPoolSize());
                statement.executeUpdate(registerStorageUnitDistSQL);
            }
        }
    }
    
    /**
     * Get Driver {@link DataSource}.
     *
     * @return Driver {@link DataSource}
     */
    public DataSource getDriverDataSource() {
        return driverContainer.getTargetDataSource(AdapterMode.CLUSTER.getValue().equalsIgnoreCase(mode) ? governanceContainer.getServerLists() : "");
    }
    
    /**
     * Close.
     */
    public void close() {
        new DataSourcePoolDestroyer(proxyContainer.getTargetDataSource(governanceContainer.getServerLists())).asyncDestroy();
        new DataSourcePoolDestroyer(getDriverDataSource()).asyncDestroy();
        containers.close();
    }
}
