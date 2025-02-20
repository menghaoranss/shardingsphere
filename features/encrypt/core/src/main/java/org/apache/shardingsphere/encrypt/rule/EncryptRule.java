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

package org.apache.shardingsphere.encrypt.rule;

import com.cedarsoftware.util.CaseInsensitiveMap;
import com.cedarsoftware.util.CaseInsensitiveSet;
import com.google.common.base.Preconditions;
import com.sphereex.dbplusengine.SphereEx;
import com.sphereex.dbplusengine.encrypt.config.rule.mode.EncryptModeType;
import com.sphereex.dbplusengine.encrypt.rule.attribute.EncryptDataNodeRuleAttribute;
import com.sphereex.dbplusengine.encrypt.rule.attribute.EncryptFailOverRuleAttribute;
import com.sphereex.dbplusengine.encrypt.rule.mode.EncryptMode;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.shardingsphere.encrypt.config.EncryptRuleConfiguration;
import org.apache.shardingsphere.encrypt.config.rule.EncryptColumnRuleConfiguration;
import org.apache.shardingsphere.encrypt.config.rule.EncryptTableRuleConfiguration;
import org.apache.shardingsphere.encrypt.constant.EncryptOrder;
import org.apache.shardingsphere.encrypt.exception.metadata.EncryptTableNotFoundException;
import org.apache.shardingsphere.encrypt.exception.metadata.MismatchedEncryptAlgorithmTypeException;
import org.apache.shardingsphere.encrypt.rule.attribute.EncryptTableMapperRuleAttribute;
import org.apache.shardingsphere.encrypt.rule.table.EncryptTable;
import org.apache.shardingsphere.encrypt.spi.EncryptAlgorithm;
import org.apache.shardingsphere.infra.algorithm.core.config.AlgorithmConfiguration;
import org.apache.shardingsphere.infra.algorithm.core.exception.AlgorithmInitializationException;
import org.apache.shardingsphere.infra.annotation.HighFrequencyInvocation;
import org.apache.shardingsphere.infra.database.DatabaseTypeEngine;
import org.apache.shardingsphere.infra.database.core.type.DatabaseType;
import org.apache.shardingsphere.infra.exception.core.ShardingSpherePreconditions;
import org.apache.shardingsphere.infra.rule.PartialRuleUpdateSupported;
import org.apache.shardingsphere.infra.rule.ShardingSphereRule;
import org.apache.shardingsphere.infra.rule.attribute.RuleAttribute;
import org.apache.shardingsphere.infra.rule.attribute.RuleAttributes;
import org.apache.shardingsphere.infra.rule.scope.DatabaseRule;
import org.apache.shardingsphere.infra.spi.type.typed.TypedSPILoader;

import javax.sql.DataSource;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

/**
 * Encrypt rule.
 */
@Slf4j
public final class EncryptRule implements DatabaseRule, PartialRuleUpdateSupported<EncryptRuleConfiguration> {
    
    private final String databaseName;
    
    private final AtomicReference<EncryptRuleConfiguration> ruleConfig = new AtomicReference<>();
    
    private final Map<String, EncryptAlgorithm> encryptors;
    
    private final Map<String, EncryptTable> tables = new CaseInsensitiveMap<>(Collections.emptyMap(), new ConcurrentHashMap<>());
    
    private final AtomicReference<RuleAttributes> attributes = new AtomicReference<>();
    
    @SphereEx
    private final Map<String, DataSource> dataSources;
    
    @SphereEx
    private final Collection<ShardingSphereRule> builtRules;
    
    @SphereEx
    private final DatabaseType databaseType;
    
    @SphereEx
    @Getter
    private final EncryptMode encryptMode;
    
    public EncryptRule(final String databaseName, final EncryptRuleConfiguration ruleConfig, @SphereEx final Map<String, DataSource> dataSources,
                       @SphereEx final Collection<ShardingSphereRule> builtRules) {
        this.databaseName = databaseName;
        this.ruleConfig.set(ruleConfig);
        encryptors = createEncryptors(ruleConfig);
        // SPEX ADDED: BEGIN
        this.dataSources = dataSources;
        this.builtRules = builtRules;
        databaseType = getDatabaseType(dataSources);
        encryptMode = new EncryptMode(ruleConfig.getEncryptMode());
        // SPEX ADDED: END
        for (EncryptTableRuleConfiguration each : ruleConfig.getTables()) {
            each.getColumns().forEach(this::checkEncryptorType);
            // SPEX CHANGED: BEGIN
            tables.put(each.getName(), new EncryptTable(each, encryptors, databaseType, encryptMode));
            // SPEX CHANGED: END
        }
        // SPEX CHANGED: BEGIN
        attributes.set(buildRuleAttributes(ruleConfig));
        // SPEX CHANGED: END
    }
    
    @SphereEx
    private DatabaseType getDatabaseType(final Map<String, DataSource> dataSources) {
        return dataSources.isEmpty() ? DatabaseTypeEngine.getDefaultStorageType() : DatabaseTypeEngine.getStorageType(dataSources.values().iterator().next());
    }
    
    private RuleAttributes buildRuleAttributes(@SphereEx final EncryptRuleConfiguration ruleConfig) {
        List<RuleAttribute> ruleAttributes = new LinkedList<>();
        // SPEX CHANGED: BEGIN
        ruleAttributes.add(new EncryptTableMapperRuleAttribute(tables.keySet(), encryptMode.isUDFEnabled() ? buildActualTableNames(tables) : Collections.emptyList()));
        // SPEX CHANGED: END
        // SPEX ADDED: BEGIN
        ruleAttributes.addAll(encryptMode.isUDFEnabled() ? buildUDFRuleAttributes(encryptMode) : Collections.emptyList());
        ruleAttributes.add(new EncryptFailOverRuleAttribute(databaseName, this));
        // SPEX ADDED: END
        return new RuleAttributes(ruleAttributes.toArray(new RuleAttribute[]{}));
    }
    
    @SphereEx
    private Collection<String> buildActualTableNames(final Map<String, EncryptTable> tables) {
        return EncryptModeType.FRONTEND == encryptMode.getType()
                ? Collections.emptyList()
                : tables.values().stream().filter(each -> each.getRenameTable().isPresent()).map(each -> each.getRenameTable().get()).collect(Collectors.toList());
    }
    
    @SphereEx
    private List<RuleAttribute> buildUDFRuleAttributes(final EncryptMode encryptMode) {
        List<RuleAttribute> result = new LinkedList<>();
        if (EncryptModeType.BACKEND == encryptMode.getType()) {
            result.add(new EncryptDataNodeRuleAttribute(databaseName, dataSources, builtRules, tables));
        }
        return result;
    }
    
    private Map<String, EncryptAlgorithm> createEncryptors(final EncryptRuleConfiguration ruleConfig) {
        Map<String, EncryptAlgorithm> result = new CaseInsensitiveMap<>(Collections.emptyMap(), new ConcurrentHashMap<>(ruleConfig.getEncryptors().size(), 1F));
        for (Entry<String, AlgorithmConfiguration> entry : ruleConfig.getEncryptors().entrySet()) {
            // SPEX CHANGED: BEGIN
            try {
                result.put(entry.getKey(), TypedSPILoader.getService(EncryptAlgorithm.class, entry.getValue().getType(), entry.getValue().getProps()));
            } catch (final AlgorithmInitializationException ex) {
                log.error("Failed to initialize encryptor `{}` in `{}`", entry.getKey(), databaseName, ex);
                throw ex;
            }
            // SPEX CHANGED: END
        }
        return result;
    }
    
    // TODO How to process changed encryptors and tables if check failed? It should check before rule change
    private void checkEncryptorType(final EncryptColumnRuleConfiguration columnRuleConfig) {
        ShardingSpherePreconditions.checkState(encryptors.containsKey(columnRuleConfig.getCipher().getEncryptorName())
                && encryptors.get(columnRuleConfig.getCipher().getEncryptorName()).getMetaData().isSupportDecrypt(),
                () -> new MismatchedEncryptAlgorithmTypeException(databaseName, "Cipher", columnRuleConfig.getCipher().getEncryptorName(), "decrypt"));
        columnRuleConfig.getAssistedQuery().ifPresent(optional -> ShardingSpherePreconditions.checkState(encryptors.containsKey(optional.getEncryptorName())
                && encryptors.get(optional.getEncryptorName()).getMetaData().isSupportEquivalentFilter(),
                () -> new MismatchedEncryptAlgorithmTypeException(databaseName, "Assisted query", columnRuleConfig.getCipher().getEncryptorName(), "equivalent filter")));
        columnRuleConfig.getLikeQuery().ifPresent(optional -> ShardingSpherePreconditions.checkState(encryptors.containsKey(optional.getEncryptorName())
                && encryptors.get(optional.getEncryptorName()).getMetaData().isSupportLike(),
                () -> new MismatchedEncryptAlgorithmTypeException(databaseName, "Like query", columnRuleConfig.getCipher().getEncryptorName(), "like")));
        // SPEX ADDED: BEGIN
        columnRuleConfig.getOrderQuery().ifPresent(optional -> ShardingSpherePreconditions.checkState(encryptors.containsKey(optional.getEncryptorName())
                && encryptors.get(optional.getEncryptorName()).getMetaData().isSupportOrder(),
                () -> new MismatchedEncryptAlgorithmTypeException(databaseName, "Order query", columnRuleConfig.getCipher().getEncryptorName(), "order")));
        // SPEX ADDED: END
    }
    
    /**
     * Get all table names.
     *
     * @return all table names
     */
    public Collection<String> getAllTableNames() {
        return tables.keySet();
    }
    
    /**
     * Find encrypt table.
     *
     * @param tableName table name
     * @return encrypt table
     */
    @HighFrequencyInvocation
    public Optional<EncryptTable> findEncryptTable(final String tableName) {
        return Optional.ofNullable(tables.get(tableName));
    }
    
    /**
     * Get encrypt table.
     *
     * @param tableName table name
     * @return encrypt table
     */
    @HighFrequencyInvocation
    public EncryptTable getEncryptTable(final String tableName) {
        return findEncryptTable(tableName).orElseThrow(() -> new EncryptTableNotFoundException(tableName));
    }
    
    /**
     * Find query encryptor.
     *
     * @param tableName table name
     * @param columnName column name
     * @return query encryptor
     */
    @HighFrequencyInvocation
    public Optional<EncryptAlgorithm> findQueryEncryptor(final String tableName, final String columnName) {
        return findEncryptTable(tableName).flatMap(optional -> optional.findQueryEncryptor(columnName));
    }
    
    /**
     * Judge whether column is query with plain or not.
     *
     * @param tableName table name
     * @param logicColumnName logic column name
     * @return whether column is query with plain or not
     */
    @SphereEx
    public boolean isQueryWithPlain(final String tableName, final String logicColumnName) {
        return findEncryptTable(tableName).map(optional -> optional.isQueryWithPlain(logicColumnName)).orElse(false);
    }
    
    @Override
    public RuleAttributes getAttributes() {
        return attributes.get();
    }
    
    @Override
    public EncryptRuleConfiguration getConfiguration() {
        return ruleConfig.get();
    }
    
    @Override
    public void updateConfiguration(final EncryptRuleConfiguration toBeUpdatedRuleConfig) {
        ruleConfig.set(toBeUpdatedRuleConfig);
    }
    
    @Override
    public boolean partialUpdate(final EncryptRuleConfiguration toBeUpdatedRuleConfig) {
        Collection<String> toBeUpdatedTablesNames = toBeUpdatedRuleConfig.getTables().stream().map(EncryptTableRuleConfiguration::getName).collect(Collectors.toCollection(CaseInsensitiveSet::new));
        Collection<String> toBeAddedTableNames = toBeUpdatedTablesNames.stream().filter(each -> !tables.containsKey(each)).collect(Collectors.toList());
        if (!toBeAddedTableNames.isEmpty()) {
            toBeAddedTableNames.forEach(each -> addTableRule(each, toBeUpdatedRuleConfig));
            // SPEX CHANGED: BEGIN
            attributes.set(buildRuleAttributes(toBeUpdatedRuleConfig));
            // SPEX CHANGED: END
            return true;
        }
        Collection<String> toBeRemovedTableNames = tables.keySet().stream().filter(each -> !toBeUpdatedTablesNames.contains(each)).collect(Collectors.toList());
        if (!toBeRemovedTableNames.isEmpty()) {
            toBeRemovedTableNames.forEach(tables::remove);
            // SPEX CHANGED: BEGIN
            attributes.set(buildRuleAttributes(toBeUpdatedRuleConfig));
            // SPEX CHANGED: END
            // TODO check and remove unused INLINE encryptors
            return true;
        }
        // TODO Process update table
        // TODO Process CRUD encryptors
        return false;
    }
    
    private void addTableRule(final String tableName, final EncryptRuleConfiguration toBeUpdatedRuleConfig) {
        EncryptTableRuleConfiguration tableRuleConfig = getTableRuleConfiguration(tableName, toBeUpdatedRuleConfig);
        for (Entry<String, AlgorithmConfiguration> entry : toBeUpdatedRuleConfig.getEncryptors().entrySet()) {
            encryptors.computeIfAbsent(entry.getKey(), key -> TypedSPILoader.getService(EncryptAlgorithm.class, entry.getValue().getType(), entry.getValue().getProps()));
        }
        tableRuleConfig.getColumns().forEach(this::checkEncryptorType);
        // SPEX CHANGED: BEGIN
        tables.put(tableName, new EncryptTable(tableRuleConfig, encryptors, databaseType, encryptMode));
        // SPEX CHANGED: END
    }
    
    private EncryptTableRuleConfiguration getTableRuleConfiguration(final String tableName, final EncryptRuleConfiguration toBeUpdatedRuleConfig) {
        Optional<EncryptTableRuleConfiguration> result = toBeUpdatedRuleConfig.getTables().stream().filter(table -> table.getName().equals(tableName)).findFirst();
        Preconditions.checkState(result.isPresent());
        return result.get();
    }
    
    /**
     * Find encrypt table by actual table.
     *
     * @param actualTableName actual table name
     * @return encrypt table
     */
    @SphereEx
    public Optional<EncryptTable> findEncryptTableByActualTable(final String actualTableName) {
        if (EncryptModeType.FRONTEND == encryptMode.getType()) {
            return Optional.empty();
        }
        for (EncryptTable each : tables.values()) {
            if (each.getRenameTable().orElse("").equalsIgnoreCase(actualTableName)) {
                return Optional.of(each);
            }
        }
        return Optional.empty();
    }
    
    @Override
    public int getOrder() {
        return EncryptOrder.ORDER;
    }
}
