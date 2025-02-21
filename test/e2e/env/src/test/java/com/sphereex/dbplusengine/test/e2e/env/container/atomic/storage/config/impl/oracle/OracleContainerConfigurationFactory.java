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

package com.sphereex.dbplusengine.test.e2e.env.container.atomic.storage.config.impl.oracle;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.apache.shardingsphere.infra.database.core.type.DatabaseType;
import org.apache.shardingsphere.infra.spi.type.typed.TypedSPILoader;
import org.apache.shardingsphere.test.e2e.env.container.atomic.storage.config.StorageContainerConfiguration;
import org.apache.shardingsphere.test.e2e.env.runtime.scenario.database.DatabaseEnvironmentManager;
import org.apache.shardingsphere.test.e2e.env.runtime.scenario.path.ScenarioDataPath;
import org.apache.shardingsphere.test.e2e.env.runtime.scenario.path.ScenarioDataPath.Type;

import java.net.URL;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Oracle container configuration factory.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class OracleContainerConfigurationFactory {
    
    /**
     * Create new instance of Oracle container configuration.
     *
     * @return created instance
     */
    public static StorageContainerConfiguration newInstance() {
        return new StorageContainerConfiguration(getCommand(), getContainerEnvironments(), getMountedResources(), Collections.emptyMap(), Collections.emptyMap());
    }
    
    /**
     * Create new instance of Oracle container configuration.
     *
     * @param scenario scenario
     * @return created instance
     */
    public static StorageContainerConfiguration newInstance(final String scenario) {
        return new StorageContainerConfiguration(getCommand(), getContainerEnvironments(), getMountedResources(scenario),
                DatabaseEnvironmentManager.getDatabaseTypes(scenario, TypedSPILoader.getService(DatabaseType.class, "Oracle")),
                DatabaseEnvironmentManager.getExpectedDatabaseTypes(scenario, TypedSPILoader.getService(DatabaseType.class, "Oracle")));
    }
    
    private static String getCommand() {
        return "";
    }
    
    private static Map<String, String> getContainerEnvironments() {
        return Collections.emptyMap();
    }
    
    private static Map<String, String> getMountedResources() {
        Map<String, String> result = new HashMap<>(1, 1F);
        String path = "env/oracle/01-initdb.sql";
        URL url = Thread.currentThread().getContextClassLoader().getResource(path);
        if (null != url) {
            result.put(path, "/docker-entrypoint-initdb.d/01-initdb.sql");
        }
        return result;
    }
    
    private static Map<String, String> getMountedResources(final String scenario) {
        Map<String, String> result = new HashMap<>(3, 1F);
        result.put(new ScenarioDataPath(scenario).getInitSQLResourcePath(Type.ACTUAL, TypedSPILoader.getService(DatabaseType.class, "Oracle")) + "/01-actual-init.sql",
                "/docker-entrypoint-initdb.d/01-actual-init.sql");
        result.put(new ScenarioDataPath(scenario).getInitSQLResourcePath(Type.EXPECTED, TypedSPILoader.getService(DatabaseType.class, "Oracle")) + "/01-expected-init.sql",
                "/docker-entrypoint-initdb.d/01-expected-init.sql");
        return result;
    }
}
