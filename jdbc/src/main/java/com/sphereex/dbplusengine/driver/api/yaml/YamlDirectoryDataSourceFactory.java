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

package com.sphereex.dbplusengine.driver.api.yaml;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.sphereex.dbplusengine.driver.api.config.JDBCConfiguration;
import com.sphereex.dbplusengine.driver.yaml.YamlJDBCDatabaseConfiguration;
import com.sphereex.dbplusengine.driver.yaml.YamlJDBCGlobalConfiguration;
import com.sphereex.dbplusengine.driver.yaml.YamlMultipleJDBCConfiguration;
import com.sphereex.dbplusengine.driver.yaml.swapper.YamlMultipleJDBCConfigurationSwapper;
import org.apache.shardingsphere.driver.jdbc.core.datasource.ShardingSphereDataSource;
import org.apache.shardingsphere.infra.config.mode.ModeConfiguration;
import org.apache.shardingsphere.infra.config.rule.RuleConfiguration;
import org.apache.shardingsphere.infra.database.core.DefaultDatabase;
import org.apache.shardingsphere.infra.spi.ShardingSphereServiceLoader;
import org.apache.shardingsphere.infra.util.yaml.YamlEngine;
import org.apache.shardingsphere.infra.yaml.config.pojo.rule.YamlRuleConfiguration;
import org.apache.shardingsphere.infra.yaml.config.swapper.mode.YamlModeConfigurationSwapper;
import org.apache.shardingsphere.infra.yaml.config.swapper.rule.YamlRuleConfigurationSwapper;
import org.apache.shardingsphere.sharding.exception.metadata.MissingRequiredShardingConfigurationException;

import javax.sql.DataSource;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.Collectors;

/**
 * YAML directory data source factory.
 */
public final class YamlDirectoryDataSourceFactory {
    
    /**
     * Create data source.
     *
     * @param configContents configuration contents
     * @param connectionProps connection properties
     * @param databaseName database name
     * @return data source
     * @throws IOException IO exception
     * @throws SQLException SQL exception
     */
    public static DataSource createDataSource(final Map<String, byte[]> configContents, final Properties connectionProps, final String databaseName) throws IOException, SQLException {
        String globalConfigKey = configContents.entrySet().stream().filter(entry -> entry.getKey().startsWith("global")).findFirst().map(Map.Entry::getKey)
                .orElseThrow(() -> new MissingRequiredShardingConfigurationException("global"));
        YamlJDBCGlobalConfiguration globalConfig = loadGlobalConfiguration(configContents.get(globalConfigKey));
        Collection<YamlJDBCDatabaseConfiguration> databaseConfigs = loadDatabaseConfigurations(configContents);
        return createDataSource(databaseName, globalConfig, databaseConfigs, connectionProps);
    }
    
    private static DataSource createDataSource(final String databaseName, final YamlJDBCGlobalConfiguration globalConfig,
                                               final Collection<YamlJDBCDatabaseConfiguration> databaseConfigs, final Properties connectionProps) throws SQLException {
        JDBCConfiguration jdbcConfig = new YamlMultipleJDBCConfigurationSwapper().swap(createYamlRootConfiguration(globalConfig, databaseConfigs));
        ModeConfiguration modeConfig = null == globalConfig.getMode() ? null : new YamlModeConfigurationSwapper().swapToObject(globalConfig.getMode());
        return new ShardingSphereDataSource(Strings.isNullOrEmpty(databaseName) ? DefaultDatabase.LOGIC_NAME : databaseName, modeConfig, jdbcConfig);
    }
    
    private static YamlMultipleJDBCConfiguration createYamlRootConfiguration(final YamlJDBCGlobalConfiguration globalConfig, final Collection<YamlJDBCDatabaseConfiguration> databaseConfigs) {
        YamlMultipleJDBCConfiguration result = new YamlMultipleJDBCConfiguration();
        result.setGlobalConfiguration(globalConfig);
        result.setDatabaseConfigurations(databaseConfigs.stream().collect(Collectors.toMap(
                YamlJDBCDatabaseConfiguration::getDatabaseName, each -> each, (oldValue, currentValue) -> oldValue, LinkedHashMap::new)));
        return result;
    }
    
    private static YamlJDBCGlobalConfiguration loadGlobalConfiguration(final byte[] configContent) throws IOException {
        YamlJDBCGlobalConfiguration result = YamlEngine.unmarshal(configContent, YamlJDBCGlobalConfiguration.class);
        result.rebuild();
        return result;
    }
    
    private static Collection<YamlJDBCDatabaseConfiguration> loadDatabaseConfigurations(final Map<String, byte[]> configContents) throws IOException {
        Collection<String> loadedDatabaseNames = new HashSet<>();
        Collection<YamlJDBCDatabaseConfiguration> result = new LinkedList<>();
        for (Entry<String, byte[]> entry : configContents.entrySet()) {
            if (entry.getKey().startsWith("global")) {
                continue;
            }
            loadDatabaseConfiguration(entry.getKey(), entry.getValue()).ifPresent(optional -> {
                Preconditions.checkState(loadedDatabaseNames.add(optional.getDatabaseName()), "Database name `%s` must unique at all database configurations.", optional.getDatabaseName());
                result.add(optional);
            });
        }
        return result;
    }
    
    private static Optional<YamlJDBCDatabaseConfiguration> loadDatabaseConfiguration(final String yamlFileName, final byte[] configByte) throws IOException {
        YamlJDBCDatabaseConfiguration result = YamlEngine.unmarshal(configByte, YamlJDBCDatabaseConfiguration.class);
        if (result.isEmpty()) {
            return Optional.empty();
        }
        Preconditions.checkNotNull(result.getDatabaseName(), "Property `databaseName` in file `%s` is required.", yamlFileName);
        checkDuplicateRule(result.getRules(), yamlFileName);
        return Optional.of(result);
    }
    
    private static void checkDuplicateRule(final Collection<YamlRuleConfiguration> ruleConfigs, final String yamlFileName) {
        if (ruleConfigs.isEmpty()) {
            return;
        }
        Map<Class<? extends RuleConfiguration>, Long> ruleConfigTypeCountMap = ruleConfigs.stream()
                .collect(Collectors.groupingBy(YamlRuleConfiguration::getRuleConfigurationType, Collectors.counting()));
        Optional<Entry<Class<? extends RuleConfiguration>, Long>> duplicateRuleConfig = ruleConfigTypeCountMap.entrySet().stream().filter(each -> each.getValue() > 1).findFirst();
        if (duplicateRuleConfig.isPresent()) {
            throw new IllegalStateException(String.format("Duplicate rule tag `!%s` in file `%s`", getDuplicateRuleTagName(duplicateRuleConfig.get().getKey()), yamlFileName));
        }
    }
    
    private static YamlRuleConfigurationSwapper<?, ?> getDuplicateRuleTagName(final Class<? extends RuleConfiguration> ruleConfigClass) {
        return ShardingSphereServiceLoader.getServiceInstances(YamlRuleConfigurationSwapper.class)
                .stream().filter(each -> ruleConfigClass.equals(each.getTypeClass())).findFirst().orElseThrow(() -> new IllegalStateException("Not find rule tag name of class " + ruleConfigClass));
    }
}
