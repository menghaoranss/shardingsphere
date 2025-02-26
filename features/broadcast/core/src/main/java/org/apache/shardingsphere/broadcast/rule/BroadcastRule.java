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

package org.apache.shardingsphere.broadcast.rule;

import com.cedarsoftware.util.CaseInsensitiveMap;
import com.cedarsoftware.util.CaseInsensitiveSet;
import com.sphereex.dbplusengine.SphereEx;
import com.sphereex.dbplusengine.broadcast.config.keygen.BroadcastKeyGenerateStrategyConfiguration;
import lombok.Getter;
import lombok.SneakyThrows;
import org.apache.shardingsphere.broadcast.config.BroadcastRuleConfiguration;
import org.apache.shardingsphere.broadcast.constant.BroadcastOrder;
import org.apache.shardingsphere.broadcast.rule.attribute.BroadcastDataNodeRuleAttribute;
import org.apache.shardingsphere.broadcast.rule.attribute.BroadcastTableNamesRuleAttribute;
import org.apache.shardingsphere.infra.algorithm.core.context.AlgorithmSQLContext;
import org.apache.shardingsphere.infra.algorithm.core.exception.MissingRequiredAlgorithmException;
import org.apache.shardingsphere.infra.algorithm.keygen.core.KeyGenerateAlgorithm;
import org.apache.shardingsphere.infra.annotation.HighFrequencyInvocation;
import org.apache.shardingsphere.infra.database.core.type.DatabaseType;
import org.apache.shardingsphere.infra.exception.core.ShardingSpherePreconditions;
import org.apache.shardingsphere.infra.exception.core.external.sql.identifier.SQLExceptionIdentifier;
import org.apache.shardingsphere.infra.instance.ComputeNodeInstanceContext;
import org.apache.shardingsphere.infra.instance.ComputeNodeInstanceContextAware;
import org.apache.shardingsphere.infra.metadata.database.resource.PhysicalDataSourceAggregator;
import org.apache.shardingsphere.infra.metadata.database.rule.RuleMetaData;
import org.apache.shardingsphere.infra.rule.ShardingSphereRule;
import org.apache.shardingsphere.infra.rule.attribute.RuleAttributes;
import org.apache.shardingsphere.infra.rule.attribute.datasource.aggregate.AggregatedDataSourceRuleAttribute;
import org.apache.shardingsphere.infra.rule.scope.DatabaseRule;
import org.apache.shardingsphere.infra.spi.type.typed.TypedSPILoader;

import javax.sql.DataSource;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Optional;

/**
 * Broadcast rule.
 */
@Getter
public final class BroadcastRule implements DatabaseRule, @SphereEx AutoCloseable {
    
    private final BroadcastRuleConfiguration configuration;
    
    private final Collection<String> dataSourceNames;
    
    private final Collection<String> tables;
    
    private final RuleAttributes attributes;
    
    @SphereEx
    private final Collection<String> actualDataSourceNames;
    
    @SphereEx
    private final Map<String, BroadcastKeyGenerateStrategyConfiguration> keyGenerateStrategies = new CaseInsensitiveMap<>();
    
    @SphereEx
    private final Map<String, KeyGenerateAlgorithm> keyGenerators = new CaseInsensitiveMap<>();
    
    public BroadcastRule(final BroadcastRuleConfiguration config, final Map<String, DataSource> dataSources, final Collection<ShardingSphereRule> builtRules,
                         @SphereEx final String databaseName, @SphereEx final DatabaseType protocolType, @SphereEx final ComputeNodeInstanceContext computeNodeInstanceContext) {
        configuration = config;
        Map<String, DataSource> aggregatedDataSources = new RuleMetaData(builtRules).findAttribute(AggregatedDataSourceRuleAttribute.class)
                .map(AggregatedDataSourceRuleAttribute::getAggregatedDataSources).orElseGet(() -> PhysicalDataSourceAggregator.getAggregatedDataSources(dataSources, builtRules));
        dataSourceNames = new CaseInsensitiveSet<>(aggregatedDataSources.keySet());
        tables = new CaseInsensitiveSet<>(config.getTables());
        attributes = new RuleAttributes(new BroadcastDataNodeRuleAttribute(dataSourceNames, tables),
                new BroadcastTableNamesRuleAttribute(tables), new AggregatedDataSourceRuleAttribute(aggregatedDataSources));
        // SPEX ADDED: BEGIN
        actualDataSourceNames = getActualDataSourceNames(config);
        config.getKeyGenerators().forEach((key, value) -> keyGenerators.put(key, TypedSPILoader.getService(KeyGenerateAlgorithm.class, value.getType(), value.getProps())));
        config.getKeyGenerateStrategies().forEach((key, value) -> putKeyGenerateStrategy(key, value, databaseName, protocolType, dataSources));
        keyGenerators.values().stream()
                .filter(ComputeNodeInstanceContextAware.class::isInstance).forEach(each -> ((ComputeNodeInstanceContextAware) each).setComputeNodeInstanceContext(computeNodeInstanceContext));
        // SPEX ADDED: END
    }
    
    @SphereEx
    private void putKeyGenerateStrategy(final String tableName, final BroadcastKeyGenerateStrategyConfiguration keyGenerateStrategy,
                                        final String databaseName, final DatabaseType protocolType, final Map<String, DataSource> dataSources) {
        ShardingSpherePreconditions.checkContainsKey(keyGenerators, keyGenerateStrategy.getKeyGeneratorName(),
                () -> new IllegalStateException(String.format("Can not find key generator %s", keyGenerateStrategy.getKeyGeneratorName())));
        ShardingSpherePreconditions.checkContains(tables, keyGenerateStrategy.getLogicTable(),
                () -> new MissingRequiredAlgorithmException("key generator", new SQLExceptionIdentifier(databaseName, keyGenerateStrategy.getLogicTable())));
        keyGenerateStrategies.put(tableName, keyGenerateStrategy);
    }
    
    @SphereEx
    private Collection<String> getActualDataSourceNames(final BroadcastRuleConfiguration config) {
        return config.getActualDataSourceNames().isEmpty() ? new LinkedHashSet<>(dataSourceNames) : new LinkedHashSet<>(config.getActualDataSourceNames());
    }
    
    /**
     * Get broadcast table names.
     *
     * @param logicTableNames all logic table names
     * @return broadcast table names
     */
    @HighFrequencyInvocation
    public Collection<String> getBroadcastTableNames(final Collection<String> logicTableNames) {
        Collection<String> result = new CaseInsensitiveSet<>();
        for (String each : logicTableNames) {
            if (tables.contains(each)) {
                result.add(each);
            }
        }
        return result;
    }
    
    /**
     * Get available datasource names.
     *
     * @param routingToActualDataSources whether routing to actual data sources or not
     * @return datasource names
     */
    @SphereEx
    public Collection<String> getAvailableDataSourceNames(final boolean routingToActualDataSources) {
        return routingToActualDataSources ? actualDataSourceNames : dataSourceNames;
    }
    
    /**
     * Find key generate strategy by table.
     *
     * @param tableName tableName
     * @return key generate strategy
     */
    @SphereEx
    public Optional<BroadcastKeyGenerateStrategyConfiguration> findKeyGenerateStrategyByTable(final String tableName) {
        return Optional.ofNullable(keyGenerateStrategies.get(tableName));
    }
    
    /**
     * Judge whether support auto increment or not.
     *
     * @param databaseName database name
     * @param tableName table name
     * @return whether support auto increment or not
     */
    @SphereEx
    public boolean isSupportAutoIncrement(final String databaseName, final String tableName) {
        BroadcastKeyGenerateStrategyConfiguration keyGenerateStrategy = findKeyGenerateStrategyByTable(tableName)
                .orElseThrow(() -> new MissingRequiredAlgorithmException("key generator", new SQLExceptionIdentifier(databaseName, tableName)));
        return keyGenerators.get(keyGenerateStrategy.getKeyGeneratorName()).isSupportAutoIncrement();
    }
    
    /**
     * Find the Generated key of logic table.
     *
     * @param algorithmSQLContext algorithm SQL context
     * @param count count
     * @return generated key
     */
    @SphereEx
    public Collection<? extends Comparable<?>> generateKeys(final AlgorithmSQLContext algorithmSQLContext, final int count) {
        BroadcastKeyGenerateStrategyConfiguration keyGenerateStrategy = findKeyGenerateStrategyByTable(algorithmSQLContext.getTableName())
                .orElseThrow(() -> new MissingRequiredAlgorithmException("key generator", new SQLExceptionIdentifier(algorithmSQLContext.getDatabaseName(), algorithmSQLContext.getTableName())));
        return keyGenerators.get(keyGenerateStrategy.getKeyGeneratorName()).generateKeys(algorithmSQLContext, count);
    }
    
    @SphereEx
    @SneakyThrows(Exception.class)
    @Override
    public void close() {
        for (KeyGenerateAlgorithm each : keyGenerators.values()) {
            if (each instanceof AutoCloseable) {
                ((AutoCloseable) each).close();
            }
        }
    }
    
    @Override
    public int getOrder() {
        return BroadcastOrder.ORDER;
    }
}
