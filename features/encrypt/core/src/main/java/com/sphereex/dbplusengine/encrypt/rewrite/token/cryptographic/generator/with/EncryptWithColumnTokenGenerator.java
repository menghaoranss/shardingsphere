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

package com.sphereex.dbplusengine.encrypt.rewrite.token.cryptographic.generator.with;

import lombok.RequiredArgsConstructor;
import lombok.Setter;
import org.apache.shardingsphere.encrypt.rule.EncryptRule;
import org.apache.shardingsphere.encrypt.rule.column.EncryptColumn;
import org.apache.shardingsphere.encrypt.rule.table.EncryptTable;
import org.apache.shardingsphere.infra.annotation.HighFrequencyInvocation;
import org.apache.shardingsphere.infra.binder.context.segment.select.projection.Projection;
import org.apache.shardingsphere.infra.binder.context.segment.select.projection.impl.ColumnProjection;
import org.apache.shardingsphere.infra.binder.context.statement.SQLStatementContext;
import org.apache.shardingsphere.infra.binder.context.statement.dml.SelectStatementContext;
import org.apache.shardingsphere.infra.database.core.metadata.database.enums.QuoteCharacter;
import org.apache.shardingsphere.infra.database.core.type.DatabaseType;
import org.apache.shardingsphere.infra.exception.core.ShardingSpherePreconditions;
import org.apache.shardingsphere.infra.metadata.ShardingSphereMetaData;
import org.apache.shardingsphere.infra.metadata.database.ShardingSphereDatabase;
import org.apache.shardingsphere.infra.rewrite.sql.token.common.generator.CollectionSQLTokenGenerator;
import org.apache.shardingsphere.infra.rewrite.sql.token.common.pojo.SQLToken;
import org.apache.shardingsphere.infra.rewrite.sql.token.common.pojo.generic.SubstitutableColumnNameToken;
import org.apache.shardingsphere.sql.parser.statement.core.segment.dml.column.ColumnSegment;
import org.apache.shardingsphere.sql.parser.statement.core.segment.dml.expr.complex.CommonTableExpressionSegment;
import org.apache.shardingsphere.sql.parser.statement.core.segment.generic.OwnerSegment;
import org.apache.shardingsphere.sql.parser.statement.core.segment.generic.ParenthesesSegment;
import org.apache.shardingsphere.sql.parser.statement.core.value.identifier.IdentifierValue;

import java.util.Collection;
import java.util.LinkedList;
import java.util.Map;
import java.util.Optional;

/**
 * With column token generator for encrypt.
 */
@HighFrequencyInvocation
@RequiredArgsConstructor
@Setter
public final class EncryptWithColumnTokenGenerator implements CollectionSQLTokenGenerator<SQLStatementContext> {
    
    private final EncryptRule rule;
    
    private final Map<String, EncryptRule> databaseEncryptRules;
    
    private final ShardingSphereDatabase database;
    
    private final ShardingSphereMetaData metaData;
    
    @Override
    public boolean isGenerateSQLToken(final SQLStatementContext sqlStatementContext) {
        return sqlStatementContext instanceof SelectStatementContext && ((SelectStatementContext) sqlStatementContext).getSqlStatement().getWithSegment().isPresent()
                && containsCommonTableColumns(((SelectStatementContext) sqlStatementContext).getSqlStatement().getWithSegment().get().getCommonTableExpressions());
    }
    
    private boolean containsCommonTableColumns(final Collection<CommonTableExpressionSegment> commonTableExpressions) {
        for (CommonTableExpressionSegment each : commonTableExpressions) {
            if (!each.getColumns().isEmpty()) {
                return true;
            }
        }
        return false;
    }
    
    @Override
    public Collection<SQLToken> generateSQLTokens(final SQLStatementContext sqlStatementContext) {
        return generateSQLTokens(sqlStatementContext.getDatabaseType(), sqlStatementContext);
    }
    
    private Collection<SQLToken> generateSQLTokens(final DatabaseType databaseType, final SQLStatementContext sqlStatementContext) {
        Collection<SQLToken> result = new LinkedList<>();
        ShardingSpherePreconditions.checkState(((SelectStatementContext) sqlStatementContext).getSqlStatement().getWithSegment().isPresent(),
                () -> new UnsupportedOperationException("No common table expressions"));
        for (CommonTableExpressionSegment each : ((SelectStatementContext) sqlStatementContext).getSqlStatement().getWithSegment().get().getCommonTableExpressions()) {
            for (ColumnSegment columnSegment : each.getColumns()) {
                generateSQLTokens(databaseType, columnSegment, result).ifPresent(result::add);
            }
        }
        return result;
    }
    
    private Optional<SubstitutableColumnNameToken> generateSQLTokens(final DatabaseType databaseType, final ColumnSegment columnSegment, final Collection<SQLToken> result) {
        Optional<EncryptTable> encryptTable = getRule(columnSegment).findEncryptTable(columnSegment.getColumnBoundInfo().getOriginalTable().getValue());
        if (!encryptTable.isPresent() || !encryptTable.get().isEncryptColumn(columnSegment.getIdentifier().getValue())) {
            return Optional.empty();
        }
        return Optional
                .of(buildSubstitutableColumnNameToken(encryptTable.get().getEncryptColumn(columnSegment.getIdentifier().getValue()), columnSegment, databaseType, encryptTable.get().getTable()));
    }
    
    private EncryptRule getRule(final ColumnSegment columnSegment) {
        return databaseEncryptRules.getOrDefault(columnSegment.getColumnBoundInfo().getOriginalDatabase().getValue(), rule);
    }
    
    private SubstitutableColumnNameToken buildSubstitutableColumnNameToken(final EncryptColumn encryptColumn, final ColumnSegment columnSegment, final DatabaseType databaseType,
                                                                           final String tableName) {
        int startIndex = columnSegment.getOwner().isPresent() ? columnSegment.getOwner().get().getStopIndex() + 2 : columnSegment.getStartIndex();
        int stopIndex = columnSegment.getStopIndex();
        Collection<Projection> columnProjections = generateWithColumns(encryptColumn, columnSegment, databaseType);
        return new SubstitutableColumnNameToken(startIndex, stopIndex, columnProjections, databaseType, database, metaData);
    }
    
    private Collection<Projection> generateWithColumns(final EncryptColumn encryptColumn, final ColumnSegment columnSegment, final DatabaseType databaseType) {
        Collection<Projection> result = new LinkedList<>();
        QuoteCharacter quoteCharacter = columnSegment.getIdentifier().getQuoteCharacter();
        boolean queryWithPlain = encryptColumn.getPlain().isPresent()
                && getRule(columnSegment).isQueryWithPlain(columnSegment.getColumnBoundInfo().getOriginalTable().getValue(), columnSegment.getColumnBoundInfo().getOriginalColumn().getValue());
        IdentifierValue owner = columnSegment.getOwner().map(OwnerSegment::getIdentifier).orElse(null);
        if (queryWithPlain && encryptColumn.getPlain().isPresent()) {
            String plainName = encryptColumn.getPlain().get().getName();
            result.add(new ColumnProjection(owner, new IdentifierValue(plainName, quoteCharacter), null, databaseType));
            appendLogicColumn(columnSegment, databaseType, plainName, result, owner, quoteCharacter);
            return result;
        }
        ParenthesesSegment leftParentheses = columnSegment.getLeftParentheses().orElse(null);
        ParenthesesSegment rightParentheses = columnSegment.getRightParentheses().orElse(null);
        result.add(new ColumnProjection(owner, new IdentifierValue(encryptColumn.getName(), quoteCharacter), null, databaseType, leftParentheses, rightParentheses));
        result.add(new ColumnProjection(owner, new IdentifierValue(encryptColumn.getCipher().getName(), quoteCharacter), null, databaseType));
        encryptColumn.getAssistedQuery().ifPresent(
                optional -> result.add(new ColumnProjection(owner, new IdentifierValue(optional.getName(), quoteCharacter), null, databaseType, leftParentheses, rightParentheses)));
        encryptColumn.getLikeQuery().ifPresent(
                optional -> result.add(new ColumnProjection(owner, new IdentifierValue(optional.getName(), quoteCharacter), null, databaseType, leftParentheses, rightParentheses)));
        encryptColumn.getOrderQuery().ifPresent(optional -> result.add(new ColumnProjection(owner, new IdentifierValue(optional.getName(), quoteCharacter), null, databaseType)));
        return result;
    }
    
    private void appendLogicColumn(final ColumnSegment columnSegment, final DatabaseType databaseType, final String plainName, final Collection<Projection> projections, final IdentifierValue owner,
                                   final QuoteCharacter quoteCharacter) {
        if (!columnSegment.getColumnBoundInfo().getOriginalColumn().getValue().equalsIgnoreCase(plainName)) {
            projections.add(new ColumnProjection(owner, new IdentifierValue(columnSegment.getColumnBoundInfo().getOriginalColumn().getValue(), quoteCharacter), null, databaseType));
        }
    }
}
