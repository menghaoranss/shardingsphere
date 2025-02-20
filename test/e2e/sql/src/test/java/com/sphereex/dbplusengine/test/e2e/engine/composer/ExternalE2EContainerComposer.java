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

package com.sphereex.dbplusengine.test.e2e.engine.composer;

import com.google.common.base.Strings;
import com.sphereex.dbplusengine.infra.util.DatabaseTypeUtils;
import com.zaxxer.hikari.HikariDataSource;
import lombok.Getter;
import org.apache.shardingsphere.infra.database.core.type.DatabaseType;
import org.apache.shardingsphere.test.e2e.env.container.atomic.ITContainers;
import org.apache.shardingsphere.test.e2e.env.container.atomic.storage.DockerStorageContainer;
import org.apache.shardingsphere.test.e2e.env.container.atomic.storage.StorageContainerFactory;
import org.apache.shardingsphere.test.e2e.env.container.atomic.storage.config.impl.StorageContainerConfigurationFactory;
import org.apache.shardingsphere.test.e2e.env.container.atomic.util.StorageContainerUtils;
import org.apache.shardingsphere.test.e2e.env.runtime.DataSourceEnvironment;
import org.apache.shardingsphere.test.e2e.env.runtime.E2ETestEnvironment;
import org.apache.shardingsphere.test.e2e.env.runtime.cluster.ClusterEnvironment;

import javax.sql.DataSource;
import java.util.Collections;

/**
 * Cluster composed container.
 */
@Getter
public final class ExternalE2EContainerComposer {
    
    private final DatabaseType databaseType;
    
    private DockerStorageContainer actualStorageContainer;
    
    private DockerStorageContainer expectedStorageContainer;
    
    public ExternalE2EContainerComposer(final DatabaseType databaseType, final ITContainers itContainers) {
        this.databaseType = databaseType;
        if (ClusterEnvironment.Type.DOCKER == E2ETestEnvironment.getInstance().getClusterEnvironment().getType()) {
            actualStorageContainer = (DockerStorageContainer) StorageContainerFactory.newInstance(databaseType, "", StorageContainerConfigurationFactory.newInstance(databaseType));
            actualStorageContainer.setName(databaseType.getType() + ".actual");
            itContainers.registerContainer(actualStorageContainer);
            actualStorageContainer.setNetworkAliases(Collections.singletonList(databaseType.getType() + ".actual"));
            expectedStorageContainer = (DockerStorageContainer) StorageContainerFactory.newInstance(databaseType, "", StorageContainerConfigurationFactory.newInstance(databaseType));
            expectedStorageContainer.setName(databaseType.getType() + ".expected");
            itContainers.registerContainer(expectedStorageContainer);
            expectedStorageContainer.setNetworkAliases(Collections.singletonList(databaseType.getType() + ".expected"));
        }
        itContainers.start();
    }
    
    /**
     * Create actual data source.
     *
     * @param dataSourceName data source name
     * @return actual data source
     */
    public DataSource createActualDataSource(final String dataSourceName) {
        DataSource dataSource;
        if (ClusterEnvironment.Type.DOCKER == E2ETestEnvironment.getInstance().getClusterEnvironment().getType()) {
            dataSource = StorageContainerUtils.generateDataSource(actualStorageContainer.getJdbcUrl(dataSourceName), actualStorageContainer.getUsername(), actualStorageContainer.getPassword());
        } else {
            dataSource = StorageContainerUtils.generateDataSource(DataSourceEnvironment.getURL(databaseType, E2ETestEnvironment.getInstance().getNativeHost(),
                    Integer.parseInt(E2ETestEnvironment.getInstance().getNativePort().split(",")[0]), dataSourceName),
                    E2ETestEnvironment.getInstance().getNativeUsername(), E2ETestEnvironment.getInstance().getNativePassword());
        }
        initDialectDataSource(dataSourceName, dataSource);
        return dataSource;
    }
    
    /**
     * Create expected data source.
     *
     * @param dataSourceName data source name
     * @return expected data source
     */
    public DataSource createExpectedDataSource(final String dataSourceName) {
        DataSource dataSource;
        if (ClusterEnvironment.Type.DOCKER == E2ETestEnvironment.getInstance().getClusterEnvironment().getType()) {
            dataSource = StorageContainerUtils.generateDataSource(expectedStorageContainer.getJdbcUrl(dataSourceName), expectedStorageContainer.getUsername(), expectedStorageContainer.getPassword());
        } else {
            dataSource = StorageContainerUtils.generateDataSource(DataSourceEnvironment.getURL(databaseType, E2ETestEnvironment.getInstance().getNativeHost(),
                    Integer.parseInt(E2ETestEnvironment.getInstance().getNativePort().split(",")[1]), dataSourceName),
                    E2ETestEnvironment.getInstance().getNativeUsername(), E2ETestEnvironment.getInstance().getNativePassword());
        }
        initDialectDataSource(dataSourceName, dataSource);
        return dataSource;
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
}
