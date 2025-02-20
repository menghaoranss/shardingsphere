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

package org.apache.shardingsphere.test.e2e.container.compose.mode;

import com.sphereex.dbplusengine.SphereEx;
import com.sphereex.dbplusengine.infra.util.DatabaseTypeUtils;
import com.sphereex.dbplusengine.test.e2e.env.container.atomic.storage.impl.HeterogeneousContainer;
import org.apache.shardingsphere.infra.database.core.type.DatabaseType;
import org.apache.shardingsphere.infra.spi.type.typed.TypedSPILoader;
import org.apache.shardingsphere.test.e2e.container.compose.ContainerComposer;
import org.apache.shardingsphere.test.e2e.container.config.ProxyClusterContainerConfigurationFactory;
import org.apache.shardingsphere.test.e2e.env.container.atomic.DockerITContainer;
import org.apache.shardingsphere.test.e2e.env.container.atomic.ITContainers;
import org.apache.shardingsphere.test.e2e.env.container.atomic.adapter.AdapterContainer;
import org.apache.shardingsphere.test.e2e.env.container.atomic.adapter.AdapterContainerFactory;
import org.apache.shardingsphere.test.e2e.env.container.atomic.adapter.config.AdaptorContainerConfiguration;
import org.apache.shardingsphere.test.e2e.env.container.atomic.enums.AdapterMode;
import org.apache.shardingsphere.test.e2e.env.container.atomic.enums.AdapterType;
import org.apache.shardingsphere.test.e2e.env.container.atomic.governance.GovernanceContainer;
import org.apache.shardingsphere.test.e2e.env.container.atomic.governance.GovernanceContainerFactory;
import org.apache.shardingsphere.test.e2e.env.container.atomic.storage.StorageContainer;
import org.apache.shardingsphere.test.e2e.env.container.atomic.storage.StorageContainerFactory;
import org.apache.shardingsphere.test.e2e.env.container.atomic.storage.config.impl.StorageContainerConfigurationFactory;
import org.apache.shardingsphere.test.e2e.env.container.atomic.util.AdapterContainerUtils;
import org.apache.shardingsphere.test.e2e.env.runtime.scenario.database.DatabaseEnvironmentManager;

import javax.sql.DataSource;
import java.util.Map;

/**
 * Cluster composed container.
 */
public final class ClusterContainerComposer implements ContainerComposer {
    
    private final ITContainers containers;
    
    private final GovernanceContainer governanceContainer;
    
    private final StorageContainer storageContainer;
    
    private final AdapterContainer adapterContainer;
    
    public ClusterContainerComposer(final String scenario, final DatabaseType databaseType, final AdapterMode adapterMode, final AdapterType adapterType) {
        containers = new ITContainers(scenario);
        // TODO support other types of governance
        governanceContainer = containers.registerContainer(GovernanceContainerFactory.newInstance("ZooKeeper"));
        // SPEX CHANGED: BEGIN
        storageContainer = containers.registerContainer(createStorageContainer(scenario, databaseType));
        AdaptorContainerConfiguration containerConfig = ProxyClusterContainerConfigurationFactory.newInstance(scenario, databaseType, AdapterContainerUtils.getAdapterContainerImage());
        DatabaseType protocolType = getProtocolType(scenario, databaseType);
        AdapterContainer adapterContainer = AdapterContainerFactory.newInstance(adapterMode, adapterType, protocolType, scenario, containerConfig, storageContainer);
        // SPEX CHANGED: END
        if (adapterContainer instanceof DockerITContainer) {
            ((DockerITContainer) adapterContainer).dependsOn(governanceContainer, storageContainer);
        }
        this.adapterContainer = containers.registerContainer(adapterContainer);
    }
    
    @SphereEx
    private StorageContainer createStorageContainer(final String scenario, final DatabaseType databaseType) {
        if (DatabaseEnvironmentManager.isHeterogeneousDatabase(scenario, databaseType)) {
            HeterogeneousContainer result = new HeterogeneousContainer(scenario, databaseType);
            result.getStorageContainers().forEach(containers::registerContainer);
            return result;
        }
        return StorageContainerFactory.newInstance(databaseType, StorageContainerConfigurationFactory.newInstance(databaseType, scenario));
    }
    
    @SphereEx
    private DatabaseType getProtocolType(final String scenario, final DatabaseType databaseType) {
        // TODO getProtocolType according to global.yaml
        return "sphereex_dual_write".equalsIgnoreCase(scenario) && DatabaseTypeUtils.isOracleDatabase(databaseType) ? TypedSPILoader.getService(DatabaseType.class, "MySQL")
                : databaseType;
    }
    
    @Override
    public void start() {
        containers.start();
    }
    
    @Override
    public DataSource getTargetDataSource() {
        return adapterContainer.getTargetDataSource(governanceContainer.getServerLists());
    }
    
    @Override
    public Map<String, DataSource> getActualDataSourceMap() {
        return storageContainer.getActualDataSourceMap();
    }
    
    @Override
    public Map<String, DataSource> getExpectedDataSourceMap() {
        return storageContainer.getExpectedDataSourceMap();
    }
    
    @Override
    public void stop() {
        containers.stop();
    }
}
