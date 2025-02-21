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

package org.apache.shardingsphere.test.e2e.env.container.atomic.storage.config.impl.mysql;

import com.sphereex.dbplusengine.SphereEx;
import com.sphereex.dbplusengine.test.e2e.env.container.atomic.util.ConfigPlaceholderReplacer;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.apache.shardingsphere.infra.database.core.type.DatabaseType;
import org.apache.shardingsphere.infra.spi.type.typed.TypedSPILoader;
import org.apache.shardingsphere.test.e2e.env.container.atomic.storage.config.StorageContainerConfiguration;
import org.apache.shardingsphere.test.e2e.env.container.atomic.storage.impl.MySQLContainer;
import org.apache.shardingsphere.test.e2e.env.container.atomic.util.ContainerUtils;
import org.apache.shardingsphere.test.e2e.env.runtime.scenario.database.DatabaseEnvironmentManager;
import org.apache.shardingsphere.test.e2e.env.runtime.scenario.path.ScenarioDataPath;
import org.apache.shardingsphere.test.e2e.env.runtime.scenario.path.ScenarioDataPath.Type;

import java.net.URL;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * MySQL container configuration factory.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class MySQLContainerConfigurationFactory {
    
    /**
     * Create new instance of MySQL container configuration.
     *
     * @param scenario scenario
     * @return created instance
     */
    public static StorageContainerConfiguration newInstance(final String scenario) {
        // SPEX CHANGED: BEGIN
        return new StorageContainerConfiguration(getCommand(), getContainerEnvironments(), getReplacedMountedResources(getMountedResources(scenario)),
                // SPEX CHANGED: END
                DatabaseEnvironmentManager.getDatabaseTypes(scenario, TypedSPILoader.getService(DatabaseType.class, "MySQL")),
                DatabaseEnvironmentManager.getExpectedDatabaseTypes(scenario, TypedSPILoader.getService(DatabaseType.class, "MySQL")));
    }
    
    /**
     * Create new instance of MySQL container configuration with major version.
     *
     * @param majorVersion major version
     * @return created instance
     */
    public static StorageContainerConfiguration newInstance(final int majorVersion) {
        return new StorageContainerConfiguration(getCommand(), getContainerEnvironments(), getMountedResources(majorVersion), Collections.emptyMap(), Collections.emptyMap());
    }
    
    /**
     * Create new instance of MySQL container configuration.
     *
     * @return created instance
     */
    public static StorageContainerConfiguration newInstance() {
        return new StorageContainerConfiguration(getCommand(), getContainerEnvironments(), getMountedResources(), Collections.emptyMap(), Collections.emptyMap());
    }
    
    private static String getCommand() {
        return "--server-id=" + ContainerUtils.generateMySQLServerId();
    }
    
    private static Map<String, String> getContainerEnvironments() {
        Map<String, String> result = new HashMap<>(2, 1F);
        result.put("LANG", "C.UTF-8");
        result.put("MYSQL_RANDOM_ROOT_PASSWORD", "yes");
        return result;
    }
    
    private static Map<String, String> getMountedResources() {
        Map<String, String> result = new HashMap<>(1, 1F);
        String path = "env/mysql/01-initdb.sql";
        URL url = Thread.currentThread().getContextClassLoader().getResource(path);
        if (null != url) {
            result.put(path, "/docker-entrypoint-initdb.d/01-initdb.sql");
        }
        return result;
    }
    
    private static Map<String, String> getMountedResources(final int majorVersion) {
        Map<String, String> result = new HashMap<>(3, 1F);
        result.put(String.format("/env/mysql/mysql%s/my.cnf", majorVersion), MySQLContainer.MYSQL_CONF_IN_CONTAINER);
        result.put("/env/mysql/01-initdb.sql", "/docker-entrypoint-initdb.d/01-initdb.sql");
        if (majorVersion > 5) {
            result.put("/env/mysql/mysql8/02-initdb.sql", "/docker-entrypoint-initdb.d/02-initdb.sql");
        }
        // SPEX ADDED: BEGIN
        result.put("/env/mysql/03-initdb.sql", "/docker-entrypoint-initdb.d/03-initdb.sql");
        // SPEX ADDED: END
        return result;
    }
    
    private static Map<String, String> getMountedResources(final String scenario) {
        Map<String, String> result = new HashMap<>(3, 1F);
        result.put(new ScenarioDataPath(scenario).getInitSQLResourcePath(Type.ACTUAL, TypedSPILoader.getService(DatabaseType.class, "MySQL")) + "/01-actual-init.sql",
                "/docker-entrypoint-initdb.d/01-actual-init.sql");
        result.put(new ScenarioDataPath(scenario).getInitSQLResourcePath(Type.EXPECTED, TypedSPILoader.getService(DatabaseType.class, "MySQL")) + "/01-expected-init.sql",
                "/docker-entrypoint-initdb.d/01-expected-init.sql");
        result.put("/env/mysql/my.cnf", MySQLContainer.MYSQL_CONF_IN_CONTAINER);
        return result;
    }
    
    @SphereEx
    private static Map<String, String> getReplacedMountedResources(final Map<String, String> mountedResources) {
        Map<String, String> replacedResources = ConfigPlaceholderReplacer.getReplacedResources(mountedResources.keySet());
        return mountedResources.entrySet().stream().collect(Collectors.toMap(each -> replacedResources.get(each.getKey()), Map.Entry::getValue));
    }
}
