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

package org.apache.shardingsphere.encrypt.checker.config;

import com.google.common.collect.Lists;
import com.sphereex.dbplusengine.SphereEx;
import com.sphereex.dbplusengine.SphereEx.Type;
import com.sphereex.dbplusengine.encrypt.config.rule.PlainColumnItemRuleConfiguration;
import com.sphereex.dbplusengine.encrypt.config.rule.mode.EncryptModeRuleConfiguration;
import com.sphereex.dbplusengine.encrypt.config.rule.mode.EncryptModeType;
import com.sphereex.dbplusengine.encrypt.exception.metadata.EncryptPlainColumnConfigException;
import com.sphereex.dbplusengine.encrypt.exception.metadata.UnsupportedEncryptColumnTypeException;
import org.apache.shardingsphere.encrypt.config.EncryptRuleConfiguration;
import org.apache.shardingsphere.encrypt.config.rule.EncryptColumnItemRuleConfiguration;
import org.apache.shardingsphere.encrypt.config.rule.EncryptColumnRuleConfiguration;
import org.apache.shardingsphere.encrypt.config.rule.EncryptTableRuleConfiguration;
import org.apache.shardingsphere.encrypt.constant.EncryptOrder;
import org.apache.shardingsphere.encrypt.exception.metadata.MissingRequiredEncryptColumnException;
import org.apache.shardingsphere.encrypt.spi.EncryptAlgorithm;
import org.apache.shardingsphere.infra.algorithm.core.config.AlgorithmConfiguration;
import org.apache.shardingsphere.infra.algorithm.core.exception.MissingRequiredAlgorithmException;
import org.apache.shardingsphere.infra.algorithm.core.exception.UnregisteredAlgorithmException;
import org.apache.shardingsphere.infra.config.rule.checker.RuleConfigurationChecker;
import org.apache.shardingsphere.infra.database.DatabaseTypeEngine;
import org.apache.shardingsphere.infra.database.core.metadata.database.datatype.DataTypeRegistry;
import org.apache.shardingsphere.infra.exception.core.ShardingSpherePreconditions;
import org.apache.shardingsphere.infra.exception.core.external.sql.identifier.SQLExceptionIdentifier;
import org.apache.shardingsphere.infra.rule.ShardingSphereRule;
import org.apache.shardingsphere.infra.spi.type.typed.TypedSPILoader;

import javax.sql.DataSource;
import java.sql.Types;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Encrypt rule configuration checker.
 */
public final class EncryptRuleConfigurationChecker implements RuleConfigurationChecker<EncryptRuleConfiguration> {
    
    @Override
    public void check(final String databaseName, final EncryptRuleConfiguration ruleConfig, final Map<String, DataSource> dataSourceMap, final Collection<ShardingSphereRule> builtRules) {
        // SPEX CHANGED: BEGIN
        // TODO Use key manager shouldn't check encryptor here, maybe need to merge checker logic
        // checkEncryptors(ruleConfig.getEncryptors());
        checkTables(databaseName, ruleConfig.getTables(), ruleConfig.getEncryptors(), ruleConfig.getEncryptMode(), dataSourceMap);
        // SPEX CHANGED: END
    }
    
    private void checkEncryptors(final Map<String, AlgorithmConfiguration> encryptors) {
        encryptors.values().forEach(each -> TypedSPILoader.checkService(EncryptAlgorithm.class, each.getType(), each.getProps()));
    }
    
    @SphereEx(Type.MODIFY)
    private void checkTables(final String databaseName, final Collection<EncryptTableRuleConfiguration> tableRuleConfigs, final Map<String, AlgorithmConfiguration> encryptors,
                             @SphereEx final EncryptModeRuleConfiguration encryptMode, @SphereEx final Map<String, DataSource> dataSourceMap) {
        tableRuleConfigs.forEach(each -> checkColumns(databaseName, each, encryptors, encryptMode, dataSourceMap));
    }
    
    @SphereEx(Type.MODIFY)
    private void checkColumns(final String databaseName, final EncryptTableRuleConfiguration tableRuleConfig, final Map<String, AlgorithmConfiguration> encryptors,
                              @SphereEx final EncryptModeRuleConfiguration encryptMode, final @SphereEx Map<String, DataSource> dataSourceMap) {
        tableRuleConfig.getColumns().forEach(each -> checkColumn(databaseName, tableRuleConfig.getName(), each, encryptors, encryptMode, dataSourceMap));
    }
    
    private void checkColumn(final String databaseName, final String tableName, final EncryptColumnRuleConfiguration columnRuleConfig, final Map<String, AlgorithmConfiguration> encryptors,
                             @SphereEx final EncryptModeRuleConfiguration encryptMode, final @SphereEx Map<String, DataSource> dataSourceMap) {
        // SPEX ADDED: BEGIN
        columnRuleConfig.getDataType().ifPresent(optional -> checkEncryptDataType(tableName, columnRuleConfig, dataSourceMap, optional));
        // SPEX ADDED: END
        checkEncryptColumnItem(databaseName, tableName, columnRuleConfig.getName(), columnRuleConfig.getCipher(), encryptors, "Cipher");
        columnRuleConfig.getAssistedQuery().ifPresent(optional -> checkEncryptColumnItem(databaseName, tableName, columnRuleConfig.getName(), optional, encryptors, "Assist Query"));
        columnRuleConfig.getLikeQuery().ifPresent(optional -> checkEncryptColumnItem(databaseName, tableName, columnRuleConfig.getName(), optional, encryptors, "Like Query"));
        // SPEX ADDED: BEGIN
        columnRuleConfig.getOrderQuery().ifPresent(optional -> checkEncryptColumnItem(databaseName, tableName, columnRuleConfig.getName(), optional, encryptors, "Order Query"));
        columnRuleConfig.getPlain().ifPresent(optional -> checkPlainColumn(databaseName, tableName, columnRuleConfig.getName(), optional, encryptMode));
        // SPEX ADDED: END
    }
    
    @SphereEx
    private static void checkEncryptDataType(final String tableName, final EncryptColumnRuleConfiguration columnRuleConfig, final Map<String, DataSource> dataSourceMap, final String dataTypeName) {
        Optional<Integer> dataType = DataTypeRegistry.getDataType(DatabaseTypeEngine.getStorageType(dataSourceMap.values().stream().iterator().next()).getType(), dataTypeName);
        dataType.ifPresent(
                optional -> ShardingSpherePreconditions.checkNotContains(Lists.newArrayList(Types.DATE, Types.TIME, Types.TIMESTAMP, Types.TIME_WITH_TIMEZONE, Types.TIMESTAMP_WITH_TIMEZONE), optional,
                        () -> new UnsupportedEncryptColumnTypeException(tableName, columnRuleConfig.getName(), dataTypeName)));
    }
    
    private void checkEncryptColumnItem(final String databaseName, final String tableName, final String logicColumnName,
                                        final EncryptColumnItemRuleConfiguration columnItem, final Map<String, AlgorithmConfiguration> encryptors, final String itemType) {
        ShardingSpherePreconditions.checkNotEmpty(columnItem.getName(),
                () -> new MissingRequiredEncryptColumnException(itemType, new SQLExceptionIdentifier(databaseName, tableName, logicColumnName)));
        ShardingSpherePreconditions.checkNotEmpty(columnItem.getEncryptorName(),
                () -> new MissingRequiredAlgorithmException(itemType + " encrypt", new SQLExceptionIdentifier(databaseName, tableName, logicColumnName)));
        Map<String, AlgorithmConfiguration> newEncryptors = new HashMap<>(encryptors.size(), 1);
        encryptors.forEach((key, value) -> newEncryptors.put(key.toLowerCase(), value));
        ShardingSpherePreconditions.checkContainsKey(newEncryptors, columnItem.getEncryptorName().toLowerCase(),
                () -> new UnregisteredAlgorithmException(itemType + " encrypt", columnItem.getEncryptorName(), new SQLExceptionIdentifier(databaseName, tableName, logicColumnName)));
    }
    
    @SphereEx
    private void checkPlainColumn(final String databaseName, final String tableName, final String logicColumnName, final PlainColumnItemRuleConfiguration plainColumnConfig,
                                  final EncryptModeRuleConfiguration encryptMode) {
        if (null != encryptMode && isConfigFrontendUseOriginalSQLWhenCipherQueryFailed(encryptMode)) {
            ShardingSpherePreconditions.checkState(logicColumnName.equalsIgnoreCase(plainColumnConfig.getName()),
                    () -> new EncryptPlainColumnConfigException(plainColumnConfig.getName(), logicColumnName));
        }
        ShardingSpherePreconditions.checkNotEmpty(plainColumnConfig.getName(),
                () -> new MissingRequiredEncryptColumnException("Plain", new SQLExceptionIdentifier(databaseName, tableName, logicColumnName)));
    }
    
    @SphereEx
    private boolean isConfigFrontendUseOriginalSQLWhenCipherQueryFailed(final EncryptModeRuleConfiguration encryptMode) {
        boolean useOriginalSQLWhenCipherQueryFailed = Boolean.parseBoolean(encryptMode.getProps().getOrDefault("use-original-sql-when-cipher-query-failed", "false").toString());
        return EncryptModeType.FRONTEND == encryptMode.getType() && useOriginalSQLWhenCipherQueryFailed;
    }
    
    @Override
    public Collection<String> getTableNames(final EncryptRuleConfiguration ruleConfig) {
        return ruleConfig.getTables().stream().map(EncryptTableRuleConfiguration::getName).collect(Collectors.toList());
    }
    
    @Override
    public int getOrder() {
        return EncryptOrder.ORDER;
    }
    
    @Override
    public Class<EncryptRuleConfiguration> getTypeClass() {
        return EncryptRuleConfiguration.class;
    }
}
