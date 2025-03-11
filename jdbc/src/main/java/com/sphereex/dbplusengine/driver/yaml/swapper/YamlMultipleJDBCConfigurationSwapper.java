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

package com.sphereex.dbplusengine.driver.yaml.swapper;

import com.sphereex.dbplusengine.driver.yaml.YamlJDBCDatabaseConfiguration;
import com.sphereex.dbplusengine.driver.yaml.YamlJDBCGlobalConfiguration;
import com.sphereex.dbplusengine.driver.yaml.YamlMultipleJDBCConfiguration;
import com.sphereex.dbplusengine.driver.api.config.JDBCConfiguration;
import com.sphereex.dbplusengine.driver.api.config.JDBCGlobalConfiguration;
import org.apache.shardingsphere.infra.config.database.DatabaseConfiguration;
import org.apache.shardingsphere.infra.config.database.impl.DataSourceProvidedDatabaseConfiguration;
import org.apache.shardingsphere.infra.config.rule.RuleConfiguration;
import org.apache.shardingsphere.infra.yaml.config.swapper.resource.YamlDataSourceConfigurationSwapper;
import org.apache.shardingsphere.infra.yaml.config.swapper.rule.YamlRuleConfigurationSwapperEngine;

import javax.sql.DataSource;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;

/**
 * YAML multiple JDBC configuration swapper.
 */
public final class YamlMultipleJDBCConfigurationSwapper {
    
    private final YamlDataSourceConfigurationSwapper dataSourceConfigSwapper = new YamlDataSourceConfigurationSwapper();
    
    private final YamlRuleConfigurationSwapperEngine ruleConfigSwapperEngine = new YamlRuleConfigurationSwapperEngine();
    
    /**
     * Swap YAML JDBC configuration to JDBC configuration.
     *
     * @param yamlConfig YAML JDBC configuration
     * @return JDBC configuration
     */
    public JDBCConfiguration swap(final YamlMultipleJDBCConfiguration yamlConfig) {
        Map<String, DatabaseConfiguration> databaseConfigs = swapDatabaseConfigurations(yamlConfig.getDatabaseConfigurations());
        JDBCGlobalConfiguration globalConfig = swapGlobalConfiguration(yamlConfig.getGlobalConfiguration());
        return new JDBCConfiguration(databaseConfigs, globalConfig);
    }
    
    private Map<String, DatabaseConfiguration> swapDatabaseConfigurations(final Map<String, YamlJDBCDatabaseConfiguration> databaseConfigs) {
        Map<String, DatabaseConfiguration> result = new LinkedHashMap<>(databaseConfigs.size(), 1F);
        for (Entry<String, YamlJDBCDatabaseConfiguration> entry : databaseConfigs.entrySet()) {
            Map<String, DataSource> dataSources = swapDataSourceConfigurations(entry.getValue().getDataSources());
            Collection<RuleConfiguration> databaseRuleConfigs = ruleConfigSwapperEngine.swapToRuleConfigurations(entry.getValue().getRules());
            result.put(entry.getKey(), new DataSourceProvidedDatabaseConfiguration(dataSources, databaseRuleConfigs));
        }
        return result;
    }
    
    private Map<String, DataSource> swapDataSourceConfigurations(final Map<String, Map<String, Object>> yamlConfigs) {
        return dataSourceConfigSwapper.swapToDataSources(yamlConfigs);
    }
    
    private JDBCGlobalConfiguration swapGlobalConfiguration(final YamlJDBCGlobalConfiguration yamlConfig) {
        Collection<RuleConfiguration> ruleConfigs = ruleConfigSwapperEngine.swapToRuleConfigurations(yamlConfig.getRules());
        return new JDBCGlobalConfiguration(ruleConfigs, yamlConfig.getProps(), yamlConfig.getLabels(), yamlConfig.getLicense());
    }
}
