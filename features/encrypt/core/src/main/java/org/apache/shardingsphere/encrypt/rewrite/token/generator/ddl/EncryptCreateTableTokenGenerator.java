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

package org.apache.shardingsphere.encrypt.rewrite.token.generator.ddl;

import com.sphereex.dbplusengine.SphereEx;
import com.sphereex.dbplusengine.SphereEx.Type;
import com.sphereex.dbplusengine.encrypt.rewrite.util.EncryptDataTypeUtils;
import com.sphereex.dbplusengine.infra.rewrite.aware.ConfigurationPropertiesAware;
import com.sphereex.dbplusengine.infra.rewrite.util.ColumnCharsetUtils;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import org.apache.shardingsphere.encrypt.rule.EncryptRule;
import org.apache.shardingsphere.encrypt.rule.column.EncryptColumn;
import org.apache.shardingsphere.encrypt.rule.column.item.CipherColumnItem;
import org.apache.shardingsphere.encrypt.rule.table.EncryptTable;
import org.apache.shardingsphere.infra.binder.context.statement.SQLStatementContext;
import org.apache.shardingsphere.infra.binder.context.statement.ddl.CreateTableStatementContext;
import org.apache.shardingsphere.infra.config.props.ConfigurationProperties;
import org.apache.shardingsphere.infra.database.core.type.DatabaseType;
import org.apache.shardingsphere.infra.metadata.database.schema.model.ShardingSphereSchema;
import org.apache.shardingsphere.infra.rewrite.sql.token.common.generator.CollectionSQLTokenGenerator;
import org.apache.shardingsphere.infra.rewrite.sql.token.common.generator.aware.SchemaMetaDataAware;
import org.apache.shardingsphere.infra.rewrite.sql.token.common.pojo.SQLToken;
import org.apache.shardingsphere.infra.rewrite.sql.token.common.pojo.generic.ColumnDefinitionToken;
import org.apache.shardingsphere.infra.rewrite.sql.token.common.pojo.generic.SubstituteColumnDefinitionToken;
import org.apache.shardingsphere.sql.parser.statement.core.segment.ddl.column.ColumnDefinitionSegment;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Create table token generator for encrypt.
 */
@RequiredArgsConstructor
// SPEX ADDED: BEGIN
@Setter
// SPEX ADDED: END
public final class EncryptCreateTableTokenGenerator
        implements
            CollectionSQLTokenGenerator<CreateTableStatementContext>,
            @SphereEx SchemaMetaDataAware,
            @SphereEx ConfigurationPropertiesAware {
    
    private final EncryptRule rule;
    
    @SphereEx
    private Map<String, ShardingSphereSchema> schemas;
    
    @SphereEx
    private ShardingSphereSchema defaultSchema;
    
    @SphereEx
    private ConfigurationProperties configurationProperties;
    
    @Override
    public boolean isGenerateSQLToken(final SQLStatementContext sqlStatementContext) {
        return sqlStatementContext instanceof CreateTableStatementContext && !(((CreateTableStatementContext) sqlStatementContext).getSqlStatement()).getColumnDefinitions().isEmpty();
    }
    
    @Override
    public Collection<SQLToken> generateSQLTokens(final CreateTableStatementContext sqlStatementContext) {
        Collection<SQLToken> result = new LinkedList<>();
        String tableName = sqlStatementContext.getSqlStatement().getTable().getTableName().getIdentifier().getValue();
        EncryptTable encryptTable = rule.getEncryptTable(tableName);
        List<ColumnDefinitionSegment> columns = new ArrayList<>(sqlStatementContext.getSqlStatement().getColumnDefinitions());
        // SPEX ADDED: BEGIN
        DatabaseType databaseType = sqlStatementContext.getDatabaseType();
        ShardingSphereSchema schema = sqlStatementContext.getTablesContext().getSchemaName().map(schemas::get).orElseGet(() -> defaultSchema);
        EncryptDataTypeUtils.checkRowByteLength(sqlStatementContext, columns, databaseType, encryptTable, schema, configurationProperties);
        // SPEX ADDED: END
        for (int index = 0; index < columns.size(); index++) {
            ColumnDefinitionSegment each = columns.get(index);
            String columnName = each.getColumnName().getIdentifier().getValue();
            if (encryptTable.isEncryptColumn(columnName)) {
                @SphereEx
                String columnCharset = ColumnCharsetUtils.getColumnCharsetName(sqlStatementContext, each, schema, configurationProperties);
                // SPEX CHANGED: BEGIN
                result.add(getSubstituteColumnToken(encryptTable.getEncryptColumn(columnName), each, columns, index, columnCharset, databaseType));
                // SPEX CHANGED: END
            }
        }
        return result;
    }
    
    private SQLToken getSubstituteColumnToken(final EncryptColumn encryptColumn, final ColumnDefinitionSegment column, final List<ColumnDefinitionSegment> columns, final int index,
                                              @SphereEx final String charsetName, @SphereEx final DatabaseType databaseType) {
        // SPEX CHANGED: BEGIN
        Collection<SQLToken> columnDefinitionTokens = new LinkedList<>();
        columnDefinitionTokens.add(getCipherColumnToken(encryptColumn, column, charsetName, databaseType));
        getAssistedQueryColumnToken(encryptColumn, column, charsetName, databaseType).ifPresent(columnDefinitionTokens::add);
        getLikeQueryColumnToken(encryptColumn, column, charsetName, databaseType).ifPresent(columnDefinitionTokens::add);
        // SPEX CHANGED: END
        // SPEX ADDED: BEGIN
        getOrderQueryColumnToken(encryptColumn, column, charsetName, databaseType).ifPresent(columnDefinitionTokens::add);
        getPlainColumnToken(encryptColumn, column).ifPresent(columnDefinitionTokens::add);
        boolean lastColumn = columns.size() - 1 == index;
        int columnStopIndex = lastColumn ? column.getStopIndex() : columns.get(index + 1).getStartIndex() - 1;
        // SPEX ADDED: END
        return new SubstituteColumnDefinitionToken(column.getStartIndex(), columnStopIndex, lastColumn, columnDefinitionTokens);
    }
    
    private SQLToken getCipherColumnToken(final EncryptColumn encryptColumn, final ColumnDefinitionSegment column, @SphereEx final String charsetName,
                                          @SphereEx final DatabaseType databaseType) {
        CipherColumnItem cipherColumnItem = encryptColumn.getCipher();
        // SPEX CHANGED: BEGIN
        return new ColumnDefinitionToken(cipherColumnItem.getName(), EncryptDataTypeUtils.getDataType(cipherColumnItem, column, charsetName, databaseType), column.getStartIndex());
        // SPEX CHANGED: END
    }
    
    @SphereEx(Type.MODIFY)
    private Optional<? extends SQLToken> getAssistedQueryColumnToken(final EncryptColumn encryptColumn, final ColumnDefinitionSegment column, final String charsetName,
                                                                     final DatabaseType databaseType) {
        return encryptColumn.getAssistedQuery()
                .map(optional -> new ColumnDefinitionToken(
                        encryptColumn.getAssistedQuery().get().getName(), EncryptDataTypeUtils.getDataType(encryptColumn.getAssistedQuery().get(), column, charsetName, databaseType),
                        column.getStartIndex()));
    }
    
    @SphereEx(Type.MODIFY)
    private Optional<? extends SQLToken> getLikeQueryColumnToken(final EncryptColumn encryptColumn, final ColumnDefinitionSegment column, final String charsetName, final DatabaseType databaseType) {
        return encryptColumn.getLikeQuery()
                .map(optional -> new ColumnDefinitionToken(
                        encryptColumn.getLikeQuery().get().getName(), EncryptDataTypeUtils.getDataType(encryptColumn.getLikeQuery().get(), column, charsetName, databaseType, true),
                        column.getStartIndex()));
    }
    
    @SphereEx
    private Optional<? extends SQLToken> getOrderQueryColumnToken(final EncryptColumn encryptColumn, final ColumnDefinitionSegment column, final String charsetName, final DatabaseType databaseType) {
        return encryptColumn.getOrderQuery()
                .map(optional -> new ColumnDefinitionToken(
                        encryptColumn.getOrderQuery().get().getName(), EncryptDataTypeUtils.getDataType(encryptColumn.getOrderQuery().get(), column, charsetName, databaseType),
                        column.getStartIndex()));
    }
    
    @SphereEx
    private Optional<? extends SQLToken> getPlainColumnToken(final EncryptColumn encryptColumn, final ColumnDefinitionSegment column) {
        Optional<ColumnDefinitionToken> encryptConfigDataTypeToken = findPlainConfigDataTypeToken(encryptColumn, column);
        if (encryptConfigDataTypeToken.isPresent()) {
            return encryptConfigDataTypeToken;
        }
        return encryptColumn.getPlain().map(optional -> new ColumnDefinitionToken(encryptColumn.getPlain().get().getName(),
                column.getText().substring(column.getColumnName().getStopIndex() - column.getColumnName().getStartIndex() + 1).trim(), column.getStartIndex()));
    }
    
    @SphereEx
    private Optional<ColumnDefinitionToken> findPlainConfigDataTypeToken(final EncryptColumn encryptColumn, final ColumnDefinitionSegment column) {
        return encryptColumn.getPlain().isPresent() && null != encryptColumn.getDataType()
                ? Optional.of(new ColumnDefinitionToken(encryptColumn.getPlain().get().getName(), encryptColumn.getDataType(), column.getStartIndex()))
                : Optional.empty();
    }
}
