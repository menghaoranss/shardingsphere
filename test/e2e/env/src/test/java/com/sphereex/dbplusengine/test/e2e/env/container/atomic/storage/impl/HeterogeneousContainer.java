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

import lombok.Getter;
import org.apache.shardingsphere.infra.database.core.type.DatabaseType;
import org.apache.shardingsphere.test.e2e.env.container.atomic.storage.EmbeddedStorageContainer;
import org.apache.shardingsphere.test.e2e.env.container.atomic.storage.StorageContainer;
import org.apache.shardingsphere.test.e2e.env.container.atomic.storage.StorageContainerFactory;
import org.apache.shardingsphere.test.e2e.env.container.atomic.storage.config.impl.StorageContainerConfigurationFactory;
import org.apache.shardingsphere.test.e2e.env.runtime.scenario.database.DatabaseEnvironmentManager;
import org.testcontainers.lifecycle.Startable;

import javax.sql.DataSource;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.Map;

/**
 * Heterogeneous container.
 */
@Getter
public final class HeterogeneousContainer extends EmbeddedStorageContainer {
    
    private final Collection<StorageContainer> storageContainers = new LinkedList<>();
    
    private final Map<String, DataSource> actualDataSourceMap = new LinkedHashMap<>();
    
    private final Map<String, DataSource> expectedDataSourceMap = new LinkedHashMap<>();
    
    public HeterogeneousContainer(final String scenario, final DatabaseType databaseType) {
        super(databaseType, scenario);
        new LinkedHashSet<>(DatabaseEnvironmentManager.getDatabaseTypes(scenario, databaseType).values()).forEach(each -> addStorageContainer(scenario, each));
        new LinkedHashSet<>(DatabaseEnvironmentManager.getExpectedDatabaseTypes(scenario, databaseType).values()).forEach(each -> addStorageContainer(scenario, each));
    }
    
    private void addStorageContainer(final String scenario, final DatabaseType databaseType) {
        storageContainers.add(StorageContainerFactory.newInstance(databaseType, "", StorageContainerConfigurationFactory.newInstance(databaseType, scenario)));
    }
    
    @Override
    public void start() {
        storageContainers.forEach(Startable::start);
        storageContainers.forEach(each -> actualDataSourceMap.putAll(each.getActualDataSourceMap()));
        storageContainers.forEach(each -> expectedDataSourceMap.putAll(each.getExpectedDataSourceMap()));
    }
    
    @Override
    public void stop() {
        storageContainers.forEach(Startable::stop);
    }
}
