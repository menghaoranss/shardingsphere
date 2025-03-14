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

import com.google.common.base.Strings;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.apache.shardingsphere.driver.jdbc.core.datasource.ShardingSphereDataSource;
import org.apache.shardingsphere.driver.yaml.YamlJDBCConfiguration;
import org.apache.shardingsphere.infra.config.mode.ModeConfiguration;
import org.apache.shardingsphere.infra.config.rule.RuleConfiguration;
import org.apache.shardingsphere.infra.database.core.DefaultDatabase;
import org.apache.shardingsphere.infra.util.yaml.YamlEngine;
import org.apache.shardingsphere.infra.yaml.config.swapper.mode.YamlModeConfigurationSwapper;
import org.apache.shardingsphere.infra.yaml.config.swapper.resource.YamlDataSourceConfigurationSwapper;
import org.apache.shardingsphere.infra.yaml.config.swapper.rule.YamlRuleConfigurationSwapperEngine;

import javax.sql.DataSource;
import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;

/**
 * SphereEx data source factory for YAML.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class YamlSphereExDataSourceFactory {
    
    /**
     * Create ShardingSphere data source.
     *
     * @param yamlFile YAML file for rule configurations
     * @param connectionProps connection properties
     * @return ShardingSphere data source
     * @throws SQLException SQL exception
     * @throws IOException IO exception
     */
    public static DataSource createDataSource(final File yamlFile, final Properties connectionProps) throws SQLException, IOException {
        YamlJDBCConfiguration rootConfig = YamlEngine.unmarshal(yamlFile, YamlJDBCConfiguration.class);
        return createDataSource(new YamlDataSourceConfigurationSwapper().swapToDataSources(rootConfig.getDataSources()), rootConfig, connectionProps);
    }
    
    /**
     * Create ShardingSphere data source.
     *
     * @param yamlBytes YAML bytes for rule configurations
     * @param connectionProps connection properties
     * @return ShardingSphere data source
     * @throws SQLException SQL exception
     * @throws IOException IO exception
     */
    public static DataSource createDataSource(final byte[] yamlBytes, final Properties connectionProps) throws SQLException, IOException {
        YamlJDBCConfiguration rootConfig = YamlEngine.unmarshal(yamlBytes, YamlJDBCConfiguration.class);
        return createDataSource(new YamlDataSourceConfigurationSwapper().swapToDataSources(rootConfig.getDataSources()), rootConfig, connectionProps);
    }
    
    /**
     * Create ShardingSphere data source.
     *
     * @param dataSourceMap data source map
     * @param yamlFile YAML file for rule configurations
     * @param connectionProps connection properties
     * @return ShardingSphere data source
     * @throws SQLException SQL exception
     * @throws IOException IO exception
     */
    public static DataSource createDataSource(final Map<String, DataSource> dataSourceMap, final File yamlFile, final Properties connectionProps) throws SQLException, IOException {
        return createDataSource(dataSourceMap, YamlEngine.unmarshal(yamlFile, YamlJDBCConfiguration.class), connectionProps);
    }
    
    /**
     * Create ShardingSphere data source.
     *
     * @param dataSource data source
     * @param yamlFile YAML file for rule configurations
     * @param connectionProps connection properties
     * @return ShardingSphere data source
     * @throws SQLException SQL exception
     * @throws IOException IO exception
     */
    public static DataSource createDataSource(final DataSource dataSource, final File yamlFile, final Properties connectionProps) throws SQLException, IOException {
        return createDataSource(dataSource, YamlEngine.unmarshal(yamlFile, YamlJDBCConfiguration.class), connectionProps);
    }
    
    /**
     * Create ShardingSphere data source.
     *
     * @param dataSourceMap data source map
     * @param yamlBytes YAML bytes for rule configurations
     * @param connectionProps connection properties
     * @return ShardingSphere data source
     * @throws SQLException SQL exception
     * @throws IOException IO exception
     */
    public static DataSource createDataSource(final Map<String, DataSource> dataSourceMap, final byte[] yamlBytes, final Properties connectionProps) throws SQLException, IOException {
        return createDataSource(dataSourceMap, YamlEngine.unmarshal(yamlBytes, YamlJDBCConfiguration.class), connectionProps);
    }
    
    /**
     * Create ShardingSphere data source.
     *
     * @param dataSource data source
     * @param yamlBytes YAML bytes for rule configurations
     * @param connectionProps connection properties
     * @return ShardingSphere data source
     * @throws SQLException SQL exception
     * @throws IOException IO exception
     */
    public static DataSource createDataSource(final DataSource dataSource, final byte[] yamlBytes, final Properties connectionProps) throws SQLException, IOException {
        return createDataSource(dataSource, YamlEngine.unmarshal(yamlBytes, YamlJDBCConfiguration.class), connectionProps);
    }
    
    private static DataSource createDataSource(final DataSource dataSource, final YamlJDBCConfiguration jdbcConfig, final Properties connectionProps) throws SQLException {
        Map<String, DataSource> dataSourceMap = new LinkedHashMap<>(Collections.singletonMap(getDatabaseName(jdbcConfig.getDatabaseName()), dataSource));
        return createDataSource(dataSourceMap, jdbcConfig, connectionProps);
    }
    
    private static DataSource createDataSource(final Map<String, DataSource> dataSourceMap, final YamlJDBCConfiguration jdbcConfig, final Properties connectionProps) throws SQLException {
        jdbcConfig.rebuild();
        Collection<RuleConfiguration> ruleConfigs = new YamlRuleConfigurationSwapperEngine().swapToRuleConfigurations(jdbcConfig.getRules());
        ModeConfiguration modeConfig = null == jdbcConfig.getMode() ? null : new YamlModeConfigurationSwapper().swapToObject(jdbcConfig.getMode());
        return new ShardingSphereDataSource(
                getDatabaseName(jdbcConfig.getDatabaseName()), modeConfig, null == dataSourceMap ? new LinkedHashMap<>() : dataSourceMap, ruleConfigs, jdbcConfig.getProps());
    }
    
    private static String getDatabaseName(final String databaseName) {
        return Strings.isNullOrEmpty(databaseName) ? DefaultDatabase.LOGIC_NAME : databaseName;
    }
}
