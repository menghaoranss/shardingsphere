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

package org.apache.shardingsphere.encrypt.rule.table;

import com.cedarsoftware.util.CaseInsensitiveMap;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.sphereex.dbplusengine.SphereEx;
import com.sphereex.dbplusengine.encrypt.config.rule.mode.EncryptModeType;
import com.sphereex.dbplusengine.encrypt.rule.column.item.OrderQueryColumnItem;
import com.sphereex.dbplusengine.encrypt.rule.column.item.PlainColumnItem;
import com.sphereex.dbplusengine.encrypt.rule.mode.EncryptMode;
import lombok.Getter;
import org.apache.shardingsphere.encrypt.config.rule.EncryptColumnRuleConfiguration;
import org.apache.shardingsphere.encrypt.config.rule.EncryptTableRuleConfiguration;
import org.apache.shardingsphere.encrypt.exception.metadata.EncryptColumnNotFoundException;
import org.apache.shardingsphere.encrypt.exception.metadata.EncryptLogicColumnNotFoundException;
import org.apache.shardingsphere.encrypt.rule.column.EncryptColumn;
import org.apache.shardingsphere.encrypt.rule.column.item.AssistedQueryColumnItem;
import org.apache.shardingsphere.encrypt.rule.column.item.CipherColumnItem;
import org.apache.shardingsphere.encrypt.rule.column.item.LikeQueryColumnItem;
import org.apache.shardingsphere.encrypt.spi.EncryptAlgorithm;
import org.apache.shardingsphere.infra.annotation.HighFrequencyInvocation;
import org.apache.shardingsphere.infra.database.core.type.DatabaseType;
import org.apache.shardingsphere.infra.exception.core.ShardingSpherePreconditions;

import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;

/**
 * Encrypt table.
 */
public final class EncryptTable {
    
    @Getter
    private final String table;
    
    // SPEX ADDED: BEGIN
    @Getter
    // SPEX ADDED: END
    private final Map<String, EncryptColumn> columns;
    
    @SphereEx
    private final String renameTable;
    
    public EncryptTable(final EncryptTableRuleConfiguration config, final Map<String, EncryptAlgorithm> encryptors,
                        @SphereEx final DatabaseType databaseType, @SphereEx final EncryptMode encryptMode) {
        table = config.getName();
        // SPEX CHANGED: BEGIN
        columns = createEncryptColumns(config, encryptors, databaseType);
        // SPEX CHANGED: END
        // SPEX ADDED: BEGIN
        renameTable = EncryptModeType.BACKEND == encryptMode.getType() && Strings.isNullOrEmpty(config.getRenameTable())
                ? encryptMode.getRenameTablePrefix().orElse("") + config.getName()
                : config.getRenameTable();
        // SPEX ADDED: END
    }
    
    private Map<String, EncryptColumn> createEncryptColumns(final EncryptTableRuleConfiguration config, final Map<String, EncryptAlgorithm> encryptors, @SphereEx final DatabaseType databaseType) {
        Map<String, EncryptColumn> result = new CaseInsensitiveMap<>();
        for (EncryptColumnRuleConfiguration each : config.getColumns()) {
            // SPEX ADDED: BEGIN
            checkColumnDataType(each);
            // SPEX ADDED: END
            // SPEX CHANGED: BEGIN
            result.put(each.getName(), createEncryptColumn(each, encryptors, databaseType));
            // SPEX CHANGED: END
        }
        return result;
    }
    
    @SphereEx
    private void checkColumnDataType(final EncryptColumnRuleConfiguration columnRuleConfig) {
        if (columnRuleConfig.getDataType().isPresent()) {
            Preconditions.checkState(columnRuleConfig.getCipher().getDataType().isPresent());
            Preconditions.checkState(!columnRuleConfig.getAssistedQuery().isPresent() || columnRuleConfig.getAssistedQuery().get().getDataType().isPresent());
            Preconditions.checkState(!columnRuleConfig.getLikeQuery().isPresent() || columnRuleConfig.getLikeQuery().get().getDataType().isPresent());
        }
    }
    
    private EncryptColumn createEncryptColumn(final EncryptColumnRuleConfiguration config, final Map<String, EncryptAlgorithm> encryptors, @SphereEx final DatabaseType databaseType) {
        CipherColumnItem cipherColumnItem = new CipherColumnItem(config.getCipher().getName(), encryptors.get(config.getCipher().getEncryptorName()));
        // SPEX ADDED: BEGIN
        config.getCipher().getDataType().ifPresent(cipherColumnItem::setDataType);
        // SPEX ADDED: END
        EncryptColumn result = new EncryptColumn(config.getName(), cipherColumnItem);
        cipherColumnItem.setEncryptColumn(result);
        cipherColumnItem.setDatabaseType(databaseType);
        if (config.getAssistedQuery().isPresent()) {
            AssistedQueryColumnItem assistedQueryColumn = new AssistedQueryColumnItem(config.getAssistedQuery().get().getName(), encryptors.get(config.getAssistedQuery().get().getEncryptorName()));
            result.setAssistedQuery(assistedQueryColumn);
            // SPEX ADDED: BEGIN
            assistedQueryColumn.setEncryptColumn(result);
            config.getAssistedQuery().get().getDataType().ifPresent(assistedQueryColumn::setDataType);
            assistedQueryColumn.setDatabaseType(databaseType);
            // SPEX ADDED: END
        }
        if (config.getLikeQuery().isPresent()) {
            LikeQueryColumnItem likeQueryColumn = new LikeQueryColumnItem(config.getLikeQuery().get().getName(), encryptors.get(config.getLikeQuery().get().getEncryptorName()));
            result.setLikeQuery(likeQueryColumn);
            // SPEX ADDED: BEGIN
            likeQueryColumn.setEncryptColumn(result);
            config.getLikeQuery().get().getDataType().ifPresent(likeQueryColumn::setDataType);
            likeQueryColumn.setDatabaseType(databaseType);
            // SPEX ADDED: END
        }
        // SPEX ADDED: BEGIN
        if (config.getOrderQuery().isPresent()) {
            OrderQueryColumnItem orderQueryColumn = new OrderQueryColumnItem(config.getOrderQuery().get().getName(), encryptors.get(config.getOrderQuery().get().getEncryptorName()));
            result.setOrderQuery(orderQueryColumn);
            orderQueryColumn.setEncryptColumn(result);
            config.getOrderQuery().get().getDataType().ifPresent(orderQueryColumn::setDataType);
            orderQueryColumn.setDatabaseType(databaseType);
        }
        if (config.getPlain().isPresent()) {
            result.setPlain(new PlainColumnItem(config.getPlain().get().getName(), config.getPlain().get().isQueryWithPlain()));
        }
        config.getDataType().ifPresent(result::setDataType);
        config.getAolianColumn().ifPresent(result::setAolianColumn);
        config.getDbid().ifPresent(result::setDbid);
        // SPEX ADDED: END
        return result;
    }
    
    /**
     * Find encryptor.
     *
     * @param logicColumnName logic column name
     * @return found encryptor
     */
    @HighFrequencyInvocation
    public Optional<EncryptAlgorithm> findEncryptor(final String logicColumnName) {
        return columns.containsKey(logicColumnName) ? Optional.of(columns.get(logicColumnName).getCipher().getEncryptor()) : Optional.empty();
    }
    
    /**
     * Is encrypt column or not.
     *
     * @param logicColumnName logic column name
     * @return encrypt column or not
     */
    @HighFrequencyInvocation
    public boolean isEncryptColumn(final String logicColumnName) {
        return columns.containsKey(logicColumnName);
    }
    
    /**
     * Get encrypt column.
     *
     * @param logicColumnName logic column name
     * @return encrypt column
     */
    @HighFrequencyInvocation
    public EncryptColumn getEncryptColumn(final String logicColumnName) {
        ShardingSpherePreconditions.checkState(isEncryptColumn(logicColumnName), () -> new EncryptColumnNotFoundException(table, logicColumnName));
        return columns.get(logicColumnName);
    }
    
    /**
     * Is cipher column or not.
     *
     * @param columnName column name
     * @return cipher column or not
     */
    public boolean isCipherColumn(final String columnName) {
        return columns.values().stream().anyMatch(each -> each.getCipher().getName().equalsIgnoreCase(columnName));
    }
    
    /**
     * Get logic column by cipher column.
     *
     * @param cipherColumnName cipher column name
     * @return logic column name
     * @throws EncryptLogicColumnNotFoundException encrypt logic column not found exception
     */
    public String getLogicColumnByCipherColumn(final String cipherColumnName) {
        for (Entry<String, EncryptColumn> entry : columns.entrySet()) {
            if (entry.getValue().getCipher().getName().equalsIgnoreCase(cipherColumnName)) {
                return entry.getValue().getName();
            }
        }
        throw new EncryptLogicColumnNotFoundException(cipherColumnName);
    }
    
    /**
     * Find logic column name by cipher column name.
     *
     * @param cipherColumnName cipher column name
     * @return logic column name
     */
    @SphereEx
    public Optional<String> findLogicColumnByCipherColumn(final String cipherColumnName) {
        for (Entry<String, EncryptColumn> entry : columns.entrySet()) {
            if (entry.getValue().getCipher().getName().equals(cipherColumnName)) {
                return Optional.of(entry.getKey());
            }
        }
        return Optional.empty();
    }
    
    /**
     * Get logic column by assisted query column.
     *
     * @param assistQueryColumnName assisted query column name
     * @return logic column name
     * @throws EncryptLogicColumnNotFoundException encrypt logic column not found exception
     */
    public String getLogicColumnByAssistedQueryColumn(final String assistQueryColumnName) {
        for (Entry<String, EncryptColumn> entry : columns.entrySet()) {
            if (entry.getValue().getAssistedQuery().isPresent() && entry.getValue().getAssistedQuery().get().getName().equalsIgnoreCase(assistQueryColumnName)) {
                return entry.getValue().getName();
            }
        }
        throw new EncryptLogicColumnNotFoundException(assistQueryColumnName);
    }
    
    /**
     * Is assisted query column or not.
     *
     * @param columnName column name
     * @return assisted query column or not
     */
    public boolean isAssistedQueryColumn(final String columnName) {
        return columns.values().stream().anyMatch(each -> columnName.equalsIgnoreCase(each.getAssistedQuery().map(AssistedQueryColumnItem::getName).orElse(null)));
    }
    
    /**
     * Is like query column or not.
     *
     * @param columnName column name
     * @return like query column or not
     */
    public boolean isLikeQueryColumn(final String columnName) {
        return columns.values().stream().anyMatch(each -> columnName.equalsIgnoreCase(each.getLikeQuery().map(LikeQueryColumnItem::getName).orElse(null)));
    }
    
    /**
     * Find query encryptor.
     *
     * @param columnName column name
     * @return query encryptor
     */
    @HighFrequencyInvocation
    public Optional<EncryptAlgorithm> findQueryEncryptor(final String columnName) {
        return isEncryptColumn(columnName) ? Optional.of(getEncryptColumn(columnName).getQueryEncryptor()) : Optional.empty();
    }
    
    /**
     * Is order query column or not.
     *
     * @param columnName column name
     * @return order query column or not
     */
    @SphereEx
    public boolean isOrderQueryColumn(final String columnName) {
        return columns.values().stream().anyMatch(each -> columnName.equalsIgnoreCase(each.getOrderQuery().map(OrderQueryColumnItem::getName).orElse(null)));
    }
    
    /**
     * Is plain column or not.
     *
     * @param columnName column name
     * @return plain column or not
     */
    @SphereEx
    public boolean isPlainColumn(final String columnName) {
        return columns.values().stream().anyMatch(each -> columnName.equalsIgnoreCase(each.getPlain().map(PlainColumnItem::getName).orElse(null)));
    }
    
    /**
     * Get logic column by plain column.
     *
     * @param plainColumnName plain column name
     * @return logic column name
     * @throws EncryptLogicColumnNotFoundException encrypt logic column not found exception
     */
    @SphereEx
    public String getLogicColumnByPlainColumn(final String plainColumnName) {
        for (Entry<String, EncryptColumn> entry : columns.entrySet()) {
            if (entry.getValue().getPlain().isPresent() && entry.getValue().getPlain().get().getName().equalsIgnoreCase(plainColumnName)) {
                return entry.getValue().getName();
            }
        }
        throw new EncryptLogicColumnNotFoundException(plainColumnName);
    }
    
    /**
     * Get logic column by like query column.
     *
     * @param likeQueryColumnName like query column name
     * @return logic column name
     * @throws EncryptLogicColumnNotFoundException encrypt logic column not found exception
     */
    @SphereEx
    public String getLogicColumnByLikeQueryColumn(final String likeQueryColumnName) {
        for (Entry<String, EncryptColumn> entry : columns.entrySet()) {
            if (entry.getValue().getLikeQuery().isPresent() && entry.getValue().getLikeQuery().get().getName().equalsIgnoreCase(likeQueryColumnName)) {
                return entry.getValue().getName();
            }
        }
        throw new EncryptLogicColumnNotFoundException(likeQueryColumnName);
    }
    
    /**
     * Get logic column by order query column.
     *
     * @param orderQueryColumnName order query column name
     * @return logic column name
     * @throws EncryptLogicColumnNotFoundException encrypt logic column not found exception
     */
    @SphereEx
    public String getLogicColumnByOrderQueryColumn(final String orderQueryColumnName) {
        for (Entry<String, EncryptColumn> entry : columns.entrySet()) {
            if (entry.getValue().getOrderQuery().isPresent() && entry.getValue().getOrderQuery().get().getName().equalsIgnoreCase(orderQueryColumnName)) {
                return entry.getValue().getName();
            }
        }
        throw new EncryptLogicColumnNotFoundException(orderQueryColumnName);
    }
    
    /**
     * Judge whether column is query with plain or not.
     *
     * @param logicColumnName logic column name
     * @return whether column is query with plain or not
     */
    @SphereEx
    public boolean isQueryWithPlain(final String logicColumnName) {
        if (!isEncryptColumn(logicColumnName)) {
            return false;
        }
        EncryptColumn encryptColumn = getEncryptColumn(logicColumnName);
        if (!encryptColumn.getPlain().isPresent()) {
            return false;
        }
        return encryptColumn.getPlain().get().isQueryWithPlain();
    }
    
    /**
     * Find data type name by actual column name.
     *
     * @param actualColumnName actual column name
     * @return data type name
     */
    @SphereEx
    public Optional<String> findDataTypeNameByActualColumnName(final String actualColumnName) {
        for (Entry<String, EncryptColumn> entry : columns.entrySet()) {
            if (entry.getValue().getCipher().getName().equalsIgnoreCase(actualColumnName)) {
                return entry.getValue().getCipher().getDataType();
            }
            if (entry.getValue().getPlain().isPresent() && entry.getValue().getPlain().get().getName().equalsIgnoreCase(actualColumnName)) {
                return Optional.ofNullable(entry.getValue().getDataType());
            }
            if (entry.getValue().getAssistedQuery().isPresent() && entry.getValue().getAssistedQuery().get().getName().equalsIgnoreCase(actualColumnName)) {
                return entry.getValue().getAssistedQuery().get().getDataType();
            }
            if (entry.getValue().getLikeQuery().isPresent() && entry.getValue().getLikeQuery().get().getName().equalsIgnoreCase(actualColumnName)) {
                return entry.getValue().getLikeQuery().get().getDataType();
            }
            if (entry.getValue().getOrderQuery().isPresent() && entry.getValue().getOrderQuery().get().getName().equalsIgnoreCase(actualColumnName)) {
                return entry.getValue().getOrderQuery().get().getDataType();
            }
        }
        return Optional.empty();
    }
    
    /**
     * Get logic columns.
     *
     * @return logic column names
     */
    @SphereEx
    public Collection<String> getLogicColumns() {
        return columns.keySet();
    }
    
    /**
     * Get encrypt backend mode rename table.
     *
     * @return encrypt backend mode rename table
     */
    @SphereEx
    public Optional<String> getRenameTable() {
        return Optional.ofNullable(renameTable);
    }
    
    /**
     * Judge whether all cipher column config plain or not.
     *
     * @return all cipher column config plain or not
     */
    @SphereEx
    @HighFrequencyInvocation
    public boolean isAllCipherColumnConfigPlain() {
        for (EncryptColumn each : columns.values()) {
            if (!each.getPlain().isPresent()) {
                return false;
            }
        }
        return true;
    }
}
