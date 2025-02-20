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
import com.sphereex.dbplusengine.infra.rewrite.aware.ConfigurationPropertiesAware;
import com.sphereex.dbplusengine.encrypt.rewrite.util.EncryptDataTypeUtils;
import com.sphereex.dbplusengine.encrypt.rule.column.item.OrderQueryColumnItem;
import com.sphereex.dbplusengine.encrypt.rule.column.item.PlainColumnItem;
import com.sphereex.dbplusengine.infra.rewrite.util.ColumnCharsetUtils;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import org.apache.shardingsphere.encrypt.exception.metadata.EncryptColumnAlterException;
import org.apache.shardingsphere.encrypt.rewrite.token.pojo.EncryptAlterTableToken;
import org.apache.shardingsphere.encrypt.rewrite.token.pojo.EncryptColumnToken;
import org.apache.shardingsphere.encrypt.rule.EncryptRule;
import org.apache.shardingsphere.encrypt.rule.column.EncryptColumn;
import org.apache.shardingsphere.encrypt.rule.column.item.AssistedQueryColumnItem;
import org.apache.shardingsphere.encrypt.rule.column.item.LikeQueryColumnItem;
import org.apache.shardingsphere.encrypt.rule.table.EncryptTable;
import org.apache.shardingsphere.encrypt.spi.EncryptAlgorithm;
import org.apache.shardingsphere.infra.binder.context.statement.SQLStatementContext;
import org.apache.shardingsphere.infra.binder.context.statement.ddl.AlterTableStatementContext;
import org.apache.shardingsphere.infra.config.props.ConfigurationProperties;
import org.apache.shardingsphere.infra.exception.core.ShardingSpherePreconditions;
import org.apache.shardingsphere.infra.metadata.database.schema.model.ShardingSphereSchema;
import org.apache.shardingsphere.infra.rewrite.sql.token.common.generator.CollectionSQLTokenGenerator;
import org.apache.shardingsphere.infra.rewrite.sql.token.common.generator.aware.SchemaMetaDataAware;
import org.apache.shardingsphere.infra.rewrite.sql.token.common.pojo.SQLToken;
import org.apache.shardingsphere.infra.rewrite.sql.token.common.pojo.Substitutable;
import org.apache.shardingsphere.infra.rewrite.sql.token.common.pojo.generic.RemoveToken;
import org.apache.shardingsphere.sql.parser.statement.core.segment.ddl.column.ColumnDefinitionSegment;
import org.apache.shardingsphere.sql.parser.statement.core.segment.ddl.column.alter.AddColumnDefinitionSegment;
import org.apache.shardingsphere.sql.parser.statement.core.segment.ddl.column.alter.ChangeColumnDefinitionSegment;
import org.apache.shardingsphere.sql.parser.statement.core.segment.ddl.column.alter.DropColumnDefinitionSegment;
import org.apache.shardingsphere.sql.parser.statement.core.segment.ddl.column.alter.ModifyColumnDefinitionSegment;
import org.apache.shardingsphere.sql.parser.statement.core.segment.ddl.column.position.ColumnPositionSegment;
import org.apache.shardingsphere.sql.parser.statement.core.segment.dml.column.ColumnSegment;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Alter table token generator for encrypt.
 */
@RequiredArgsConstructor
@Setter
public final class EncryptAlterTableTokenGenerator implements CollectionSQLTokenGenerator<AlterTableStatementContext>, @SphereEx SchemaMetaDataAware, @SphereEx ConfigurationPropertiesAware {
    
    private final EncryptRule rule;
    
    @SphereEx
    private Map<String, ShardingSphereSchema> schemas;
    
    @SphereEx
    private ShardingSphereSchema defaultSchema;
    
    @SphereEx
    private ConfigurationProperties configurationProperties;
    
    @Override
    public boolean isGenerateSQLToken(final SQLStatementContext sqlStatementContext) {
        return sqlStatementContext instanceof AlterTableStatementContext;
    }
    
    @Override
    public Collection<SQLToken> generateSQLTokens(final AlterTableStatementContext sqlStatementContext) {
        String tableName = sqlStatementContext.getSqlStatement().getTable().getTableName().getIdentifier().getValue();
        EncryptTable encryptTable = rule.getEncryptTable(tableName);
        @SphereEx(Type.MODIFY)
        Collection<SQLToken> result = new LinkedList<>(getAddColumnTokens(encryptTable, sqlStatementContext.getSqlStatement().getAddColumnDefinitions(), sqlStatementContext));
        result.addAll(getModifyColumnTokens(encryptTable, sqlStatementContext.getSqlStatement().getModifyColumnDefinitions()));
        result.addAll(getChangeColumnTokens(encryptTable, sqlStatementContext.getSqlStatement().getChangeColumnDefinitions()));
        List<SQLToken> dropColumnTokens = getDropColumnTokens(encryptTable, sqlStatementContext.getSqlStatement().getDropColumnDefinitions());
        String databaseName = sqlStatementContext.getDatabaseType().getType();
        if ("SQLServer".equals(databaseName)) {
            result.addAll(mergeDropColumnStatement(dropColumnTokens, "", ""));
        } else if ("Oracle".equals(databaseName)) {
            result.addAll(mergeDropColumnStatement(dropColumnTokens, "(", ")"));
        } else {
            result.addAll(dropColumnTokens);
        }
        return result;
    }
    
    private Collection<SQLToken> getAddColumnTokens(final EncryptTable encryptTable, final Collection<AddColumnDefinitionSegment> segments,
                                                    @SphereEx final AlterTableStatementContext sqlStatementContext) {
        Collection<SQLToken> result = new LinkedList<>();
        for (AddColumnDefinitionSegment each : segments) {
            // SPEX CHANGED: BEGIN
            result.addAll(getAddColumnTokens(encryptTable, each, sqlStatementContext));
            // SPEX CHANGED: END
        }
        return result;
    }
    
    private Collection<SQLToken> getAddColumnTokens(final EncryptTable encryptTable, final AddColumnDefinitionSegment segment, @SphereEx final AlterTableStatementContext sqlStatementContext) {
        Collection<SQLToken> result = new LinkedList<>();
        @SphereEx
        ShardingSphereSchema schema = sqlStatementContext.getTablesContext().getSchemaName().map(schemas::get).orElseGet(() -> defaultSchema);
        for (ColumnDefinitionSegment each : segment.getColumnDefinitions()) {
            String columnName = each.getColumnName().getIdentifier().getValue();
            if (encryptTable.isEncryptColumn(columnName)) {
                // SPEX ADDED: BEGIN
                String columnCharset = ColumnCharsetUtils.getColumnCharsetName(sqlStatementContext, each, schema, configurationProperties);
                // SPEX ADDED: END
                // SPEX CHANGED: BEGIN
                result.addAll(getAddColumnTokens(encryptTable.getEncryptColumn(columnName), segment, each, sqlStatementContext, columnCharset));
                // SPEX CHANGED: END
            }
        }
        getAddColumnPositionToken(encryptTable, segment).ifPresent(result::add);
        return result;
    }
    
    private Collection<SQLToken> getAddColumnTokens(final EncryptColumn encryptColumn,
                                                    final AddColumnDefinitionSegment addColumnDefinitionSegment, final ColumnDefinitionSegment columnDefinitionSegment,
                                                    @SphereEx final AlterTableStatementContext sqlStatementContext, @SphereEx final String charsetName) {
        Collection<SQLToken> result = new LinkedList<>();
        result.add(new RemoveToken(columnDefinitionSegment.getStartIndex(), columnDefinitionSegment.getStopIndex()));
        // SPEX CHANGED: BEGIN
        result.add(new EncryptColumnToken(columnDefinitionSegment.getStopIndex() + 1, columnDefinitionSegment.getStopIndex(),
                encryptColumn.getCipher().getName(), EncryptDataTypeUtils.getDataType(encryptColumn.getCipher(), columnDefinitionSegment, charsetName, sqlStatementContext.getDatabaseType())));
        encryptColumn.getAssistedQuery().map(optional -> new EncryptColumnToken(addColumnDefinitionSegment.getStopIndex() + 1,
                addColumnDefinitionSegment.getStopIndex(), ", ADD COLUMN " + optional.getName(),
                EncryptDataTypeUtils.getDataType(optional, columnDefinitionSegment, charsetName, sqlStatementContext.getDatabaseType()))).ifPresent(result::add);
        encryptColumn.getLikeQuery().map(optional -> new EncryptColumnToken(addColumnDefinitionSegment.getStopIndex() + 1,
                addColumnDefinitionSegment.getStopIndex(), ", ADD COLUMN " + optional.getName(),
                EncryptDataTypeUtils.getDataType(optional, columnDefinitionSegment, charsetName, sqlStatementContext.getDatabaseType(), true))).ifPresent(result::add);
        // SPEX CHANGED: END
        // SPEX ADDED: BEGIN
        encryptColumn.getOrderQuery().map(optional -> new EncryptColumnToken(addColumnDefinitionSegment.getStopIndex() + 1,
                addColumnDefinitionSegment.getStopIndex(), ", ADD COLUMN " + optional.getName(),
                EncryptDataTypeUtils.getDataType(optional, columnDefinitionSegment, charsetName, sqlStatementContext.getDatabaseType()))).ifPresent(result::add);
        encryptColumn.getPlain().map(optional -> getPlainColumnToken(encryptColumn, addColumnDefinitionSegment, columnDefinitionSegment, optional)).ifPresent(result::add);
        // SPEX ADDED: END
        return result;
    }
    
    @SphereEx
    private SQLToken getPlainColumnToken(final EncryptColumn encryptColumn, final AddColumnDefinitionSegment addColumnDefinitionSegment,
                                         final ColumnDefinitionSegment columnDefinitionSegment, final PlainColumnItem plainColumnItem) {
        return null == encryptColumn.getDataType()
                ? new EncryptAlterTableToken(addColumnDefinitionSegment.getStopIndex() + 1, columnDefinitionSegment.getColumnName().getStopIndex(), plainColumnItem.getName(), ", ADD COLUMN")
                : new EncryptColumnToken(addColumnDefinitionSegment.getStopIndex() + 1, addColumnDefinitionSegment.getStopIndex(), ", ADD COLUMN " + plainColumnItem.getName(),
                        encryptColumn.getDataType());
    }
    
    @SphereEx
    private SQLToken getPlainColumnToken(final PlainColumnItem plainColumnItem, final EncryptColumn encryptColumn, final ChangeColumnDefinitionSegment segment) {
        return null == encryptColumn.getDataType()
                ? new EncryptAlterTableToken(segment.getStopIndex() + 1, segment.getColumnDefinition().getColumnName().getStopIndex(),
                        encryptColumn.getPlain().map(PlainColumnItem::getName).orElse(""), ", CHANGE COLUMN " + plainColumnItem.getName())
                : new EncryptColumnToken(segment.getStopIndex() + 1, segment.getStopIndex(),
                        ", CHANGE COLUMN " + plainColumnItem.getName() + " " + encryptColumn.getPlain().map(PlainColumnItem::getName).orElse(""), encryptColumn.getDataType());
    }
    
    private Optional<SQLToken> getAddColumnPositionToken(final EncryptTable encryptTable, final AddColumnDefinitionSegment segment) {
        Optional<ColumnPositionSegment> columnPositionSegment = segment.getColumnPosition().filter(optional -> null != optional.getColumnName());
        if (columnPositionSegment.isPresent()) {
            String columnName = columnPositionSegment.get().getColumnName().getIdentifier().getValue();
            if (encryptTable.isEncryptColumn(columnName)) {
                return Optional.of(getPositionColumnToken(encryptTable.getEncryptColumn(columnName), segment.getColumnPosition().get()));
            }
        }
        return Optional.empty();
    }
    
    private EncryptAlterTableToken getPositionColumnToken(final EncryptColumn encryptColumn, final ColumnPositionSegment segment) {
        return new EncryptAlterTableToken(segment.getColumnName().getStartIndex(), segment.getStopIndex(), encryptColumn.getCipher().getName(), null);
    }
    
    private Collection<SQLToken> getModifyColumnTokens(final EncryptTable encryptTable, final Collection<ModifyColumnDefinitionSegment> segments) {
        Collection<SQLToken> result = new LinkedList<>();
        for (ModifyColumnDefinitionSegment each : segments) {
            String columnName = each.getColumnDefinition().getColumnName().getIdentifier().getValue();
            // SPEX CHANGED: BEGIN
            ShardingSpherePreconditions.checkState(!encryptTable.isEncryptColumn(columnName) && !encryptTable.isPlainColumn(columnName),
                    () -> new UnsupportedOperationException("Unsupported operation 'modify' for the cipher column and plain column"));
            // SPEX CHANGED: END
            each.getColumnPosition().flatMap(optional -> getColumnPositionToken(encryptTable, optional)).ifPresent(result::add);
        }
        return result;
    }
    
    private Optional<SQLToken> getColumnPositionToken(final EncryptTable encryptTable, final ColumnPositionSegment segment) {
        if (null == segment.getColumnName()) {
            return Optional.empty();
        }
        String columnName = segment.getColumnName().getIdentifier().getValue();
        return encryptTable.isEncryptColumn(columnName) ? Optional.of(getPositionColumnToken(encryptTable.getEncryptColumn(columnName), segment)) : Optional.empty();
    }
    
    private Collection<SQLToken> getChangeColumnTokens(final EncryptTable encryptTable, final Collection<ChangeColumnDefinitionSegment> segments) {
        Collection<SQLToken> result = new LinkedList<>();
        for (ChangeColumnDefinitionSegment each : segments) {
            String columnName = each.getPreviousColumn().getIdentifier().getValue();
            // SPEX CHANGED: BEGIN
            ShardingSpherePreconditions.checkState(!encryptTable.isEncryptColumn(columnName) && !encryptTable.isPlainColumn(columnName),
                    () -> new UnsupportedOperationException("Unsupported operation 'change' for the cipher column and plain column"));
            // SPEX CHANGED: END
            result.addAll(getChangeColumnTokens(encryptTable, each));
            each.getColumnPosition().flatMap(optional -> getColumnPositionToken(encryptTable, optional)).ifPresent(result::add);
        }
        return result;
    }
    
    private Collection<SQLToken> getChangeColumnTokens(final EncryptTable encryptTable, final ChangeColumnDefinitionSegment segment) {
        String previousColumnName = segment.getPreviousColumn().getIdentifier().getValue();
        String columnName = segment.getColumnDefinition().getColumnName().getIdentifier().getValue();
        isSameEncryptColumn(encryptTable, previousColumnName, columnName);
        if (!encryptTable.isEncryptColumn(columnName) || !encryptTable.isEncryptColumn(previousColumnName)) {
            return Collections.emptyList();
        }
        Collection<SQLToken> result = new LinkedList<>();
        EncryptColumn previousEncryptColumn = encryptTable.getEncryptColumn(previousColumnName);
        EncryptColumn encryptColumn = encryptTable.getEncryptColumn(columnName);
        result.addAll(getPreviousColumnTokens(previousEncryptColumn, segment));
        result.addAll(getColumnTokens(previousEncryptColumn, encryptColumn, segment));
        return result;
    }
    
    private void isSameEncryptColumn(final EncryptTable encryptTable, final String previousColumnName, final String columnName) {
        Optional<EncryptAlgorithm> previousEncryptor = encryptTable.findEncryptor(previousColumnName);
        Optional<EncryptAlgorithm> currentEncryptor = encryptTable.findEncryptor(columnName);
        if (!previousEncryptor.isPresent() && !currentEncryptor.isPresent()) {
            return;
        }
        ShardingSpherePreconditions.checkState(previousEncryptor.equals(currentEncryptor) && checkPreviousAndAfterHasSameColumnNumber(encryptTable, previousColumnName, columnName),
                () -> new EncryptColumnAlterException(encryptTable.getTable(), columnName, previousColumnName));
    }
    
    private boolean checkPreviousAndAfterHasSameColumnNumber(final EncryptTable encryptTable, final String previousColumnName, final String columnName) {
        EncryptColumn previousEncryptColumn = encryptTable.getEncryptColumn(previousColumnName);
        EncryptColumn encryptColumn = encryptTable.getEncryptColumn(columnName);
        // SPEX ADDED: BEGIN
        if (previousEncryptColumn.getPlain().isPresent() && !encryptColumn.getPlain().isPresent()) {
            return false;
        }
        if (!previousEncryptColumn.getPlain().isPresent() && encryptColumn.getPlain().isPresent()) {
            return false;
        }
        if (previousEncryptColumn.getOrderQuery().isPresent() && !encryptColumn.getOrderQuery().isPresent()) {
            return false;
        }
        // SPEX ADDED: END
        if (previousEncryptColumn.getAssistedQuery().isPresent() && !encryptColumn.getAssistedQuery().isPresent()) {
            return false;
        }
        if (previousEncryptColumn.getLikeQuery().isPresent() && !encryptColumn.getLikeQuery().isPresent()) {
            return false;
        }
        return previousEncryptColumn.getAssistedQuery().isPresent() || !encryptColumn.getAssistedQuery().isPresent();
    }
    
    private Collection<SQLToken> getPreviousColumnTokens(final EncryptColumn previousEncryptColumn, final ChangeColumnDefinitionSegment segment) {
        Collection<SQLToken> result = new LinkedList<>();
        result.add(new RemoveToken(segment.getPreviousColumn().getStartIndex(), segment.getPreviousColumn().getStopIndex()));
        result.add(new EncryptAlterTableToken(segment.getPreviousColumn().getStopIndex() + 1, segment.getPreviousColumn().getStopIndex(), previousEncryptColumn.getCipher().getName(), null));
        return result;
    }
    
    private Collection<SQLToken> getColumnTokens(final EncryptColumn previousEncryptColumn, final EncryptColumn encryptColumn, final ChangeColumnDefinitionSegment segment) {
        Collection<SQLToken> result = new LinkedList<>();
        result.add(new RemoveToken(segment.getColumnDefinition().getColumnName().getStartIndex(), segment.getColumnDefinition().getStopIndex()));
        // SPEX CHANGED: BEGIN
        result.add(new EncryptColumnToken(segment.getColumnDefinition().getStopIndex() + 1, segment.getColumnDefinition().getStopIndex(),
                encryptColumn.getCipher().getName(), EncryptDataTypeUtils.getDataType(encryptColumn.getCipher())));
        previousEncryptColumn.getAssistedQuery().map(optional -> new EncryptColumnToken(segment.getStopIndex() + 1, segment.getStopIndex(),
                ", CHANGE COLUMN " + optional.getName() + " " + encryptColumn.getAssistedQuery().map(AssistedQueryColumnItem::getName).orElse(""),
                EncryptDataTypeUtils.getDataType(optional))).ifPresent(result::add);
        previousEncryptColumn.getLikeQuery().map(optional -> new EncryptColumnToken(segment.getStopIndex() + 1, segment.getStopIndex(),
                ", CHANGE COLUMN " + optional.getName() + " " + encryptColumn.getLikeQuery().map(LikeQueryColumnItem::getName).orElse(""),
                EncryptDataTypeUtils.getDataType(optional))).ifPresent(result::add);
        // SPEX CHANGED: END
        // SPEX ADDED: BEGIN
        previousEncryptColumn.getOrderQuery().map(optional -> new EncryptColumnToken(segment.getStopIndex() + 1, segment.getStopIndex(),
                ", CHANGE COLUMN " + optional.getName() + " " + encryptColumn.getOrderQuery().map(OrderQueryColumnItem::getName).orElse(""),
                EncryptDataTypeUtils.getDataType(optional))).ifPresent(result::add);
        previousEncryptColumn.getPlain().map(optional -> getPlainColumnToken(optional, encryptColumn, segment)).ifPresent(result::add);
        // SPEX ADDED: END
        return result;
    }
    
    private List<SQLToken> getDropColumnTokens(final EncryptTable encryptTable, final Collection<DropColumnDefinitionSegment> segments) {
        List<SQLToken> result = new ArrayList<>();
        for (DropColumnDefinitionSegment each : segments) {
            result.addAll(getDropColumnTokens(encryptTable, each));
        }
        return result;
    }
    
    private Collection<SQLToken> getDropColumnTokens(final EncryptTable encryptTable, final DropColumnDefinitionSegment segment) {
        Collection<SQLToken> result = new LinkedList<>();
        for (ColumnSegment each : segment.getColumns()) {
            // SPEX CHANGED: BEGIN
            ShardingSpherePreconditions.checkState(!encryptTable.isEncryptColumn(each.getIdentifier().getValue()) && !encryptTable.isPlainColumn(each.getIdentifier().getValue()),
                    () -> new UnsupportedOperationException("Unsupported operation 'drop' for the cipher column and plain column"));
            // SPEX CHANGED: END
        }
        return result;
    }
    
    private Collection<SQLToken> mergeDropColumnStatement(final List<SQLToken> dropSQLTokens, final String leftJoiner, final String rightJoiner) {
        Collection<SQLToken> result = new LinkedList<>();
        Collection<String> dropColumns = new LinkedList<>();
        int lastStartIndex = -1;
        for (int i = 0; i < dropSQLTokens.size(); i++) {
            SQLToken token = dropSQLTokens.get(i);
            if (token instanceof RemoveToken) {
                result.add(0 == i ? token : new RemoveToken(lastStartIndex, token.getStopIndex()));
            } else {
                EncryptAlterTableToken encryptAlterTableToken = (EncryptAlterTableToken) token;
                dropColumns.add(encryptAlterTableToken.getColumnName());
                if (i == dropSQLTokens.size() - 1) {
                    result.add(new EncryptAlterTableToken(token.getStartIndex(), encryptAlterTableToken.getStopIndex(), leftJoiner + String.join(",", dropColumns) + rightJoiner, "DROP COLUMN"));
                }
            }
            lastStartIndex = ((Substitutable) token).getStartIndex();
        }
        return result;
    }
}
