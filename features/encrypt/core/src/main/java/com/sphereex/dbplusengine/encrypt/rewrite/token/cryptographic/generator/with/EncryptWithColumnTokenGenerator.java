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

import org.apache.shardingsphere.encrypt.enums.EncryptDerivedColumnSuffix;
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
import org.apache.shardingsphere.infra.metadata.ShardingSphereMetaData;
import org.apache.shardingsphere.infra.metadata.database.ShardingSphereDatabase;
import org.apache.shardingsphere.infra.rewrite.sql.token.common.generator.CollectionSQLTokenGenerator;
import org.apache.shardingsphere.infra.rewrite.sql.token.common.pojo.SQLToken;
import org.apache.shardingsphere.infra.rewrite.sql.token.common.pojo.generic.SubstitutableColumnNameToken;
import org.apache.shardingsphere.sql.parser.statement.core.enums.TableSourceType;
import org.apache.shardingsphere.sql.parser.statement.core.segment.dml.column.ColumnSegment;
import org.apache.shardingsphere.sql.parser.statement.core.segment.dml.expr.complex.CommonTableExpressionSegment;
import org.apache.shardingsphere.sql.parser.statement.core.segment.generic.OwnerSegment;
import org.apache.shardingsphere.sql.parser.statement.core.segment.generic.ParenthesesSegment;
import org.apache.shardingsphere.sql.parser.statement.core.segment.generic.WithSegment;
import org.apache.shardingsphere.sql.parser.statement.core.value.identifier.IdentifierValue;

import java.util.Collection;
import java.util.Collections;
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
                && containsCommonTableColumns(((SelectStatementContext) sqlStatementContext).getSqlStatement().getWithSegment().get());
    }
    
    private boolean containsCommonTableColumns(final WithSegment withSegment) {
        for (CommonTableExpressionSegment each : withSegment.getCommonTableExpressions()) {
            if (!each.getColumns().isEmpty()) {
                return true;
            }
        }
        return false;
    }
    
    @Override
    public Collection<SQLToken> generateSQLTokens(final SQLStatementContext sqlStatementContext) {
        Collection<SQLToken> result = new LinkedList<>();
        Collection<CommonTableExpressionSegment> tableExpressions = ((SelectStatementContext) sqlStatementContext)
                .getSqlStatement().getWithSegment().map(WithSegment::getCommonTableExpressions).orElse(Collections.emptyList());
        for (CommonTableExpressionSegment each : tableExpressions) {
            for (ColumnSegment columnSegment : each.getColumns()) {
                generateSQLTokens(sqlStatementContext.getDatabaseType(), columnSegment).ifPresent(result::add);
            }
        }
        return result;
    }
    
    private Optional<SubstitutableColumnNameToken> generateSQLTokens(final DatabaseType databaseType, final ColumnSegment columnSegment) {
        Optional<EncryptTable> encryptTable = getRule(columnSegment).findEncryptTable(columnSegment.getColumnBoundInfo().getOriginalTable().getValue());
        if (!encryptTable.isPresent() || !encryptTable.get().isEncryptColumn(columnSegment.getIdentifier().getValue())) {
            return Optional.empty();
        }
        EncryptColumn encryptColumn = encryptTable.get().getEncryptColumn(columnSegment.getIdentifier().getValue());
        return Optional.of(buildSubstitutableColumnNameToken(encryptColumn, columnSegment, databaseType));
    }
    
    private EncryptRule getRule(final ColumnSegment columnSegment) {
        return databaseEncryptRules.getOrDefault(columnSegment.getColumnBoundInfo().getOriginalDatabase().getValue(), rule);
    }
    
    private SubstitutableColumnNameToken buildSubstitutableColumnNameToken(final EncryptColumn encryptColumn, final ColumnSegment columnSegment, final DatabaseType databaseType) {
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
            IdentifierValue plainColumnName = TableSourceType.TEMPORARY_TABLE == columnSegment.getColumnBoundInfo().getTableSourceType()
                    ? new IdentifierValue(EncryptDerivedColumnSuffix.PLAIN.getDerivedColumnName(columnSegment.getIdentifier().getValue(), databaseType), quoteCharacter)
                    : new IdentifierValue(encryptColumn.getPlain().get().getName(), quoteCharacter);
            result.add(new ColumnProjection(owner, plainColumnName, null, databaseType));
            return result;
        }
        ParenthesesSegment leftParentheses = columnSegment.getLeftParentheses().orElse(null);
        ParenthesesSegment rightParentheses = columnSegment.getRightParentheses().orElse(null);
        IdentifierValue cipherName = new IdentifierValue(EncryptDerivedColumnSuffix.CIPHER.getDerivedColumnName(columnSegment.getIdentifier().getValue(), databaseType), quoteCharacter);
        result.add(new ColumnProjection(owner, cipherName, null, databaseType));
        encryptColumn.getAssistedQuery().ifPresent(optional -> addAssistedQuery(columnSegment, databaseType, owner, quoteCharacter, leftParentheses, rightParentheses, result));
        encryptColumn.getLikeQuery().ifPresent(optional -> addLikeQuery(columnSegment, databaseType, owner, quoteCharacter, leftParentheses, rightParentheses, result));
        encryptColumn.getOrderQuery().ifPresent(optional -> addOrderQuery(columnSegment, databaseType, owner, quoteCharacter, result));
        return result;
    }
    
    private void addAssistedQuery(final ColumnSegment columnSegment, final DatabaseType databaseType, final IdentifierValue owner, final QuoteCharacter quoteCharacter,
                                  final ParenthesesSegment leftParentheses, final ParenthesesSegment rightParentheses, final Collection<Projection> result) {
        String assistedQueryName = EncryptDerivedColumnSuffix.ASSISTED_QUERY.getDerivedColumnName(columnSegment.getIdentifier().getValue(), databaseType);
        result.add(new ColumnProjection(owner, new IdentifierValue(assistedQueryName, quoteCharacter), null, databaseType, leftParentheses, rightParentheses));
    }
    
    private void addLikeQuery(final ColumnSegment columnSegment, final DatabaseType databaseType, final IdentifierValue owner, final QuoteCharacter quoteCharacter,
                              final ParenthesesSegment leftParentheses, final ParenthesesSegment rightParentheses, final Collection<Projection> result) {
        String likeQueryName = EncryptDerivedColumnSuffix.LIKE_QUERY.getDerivedColumnName(columnSegment.getIdentifier().getValue(), databaseType);
        result.add(new ColumnProjection(owner, new IdentifierValue(likeQueryName, quoteCharacter), null, databaseType, leftParentheses, rightParentheses));
    }
    
    private static void addOrderQuery(final ColumnSegment columnSegment, final DatabaseType databaseType, final IdentifierValue owner, final QuoteCharacter quoteCharacter,
                                      final Collection<Projection> result) {
        String orderQueryName = EncryptDerivedColumnSuffix.ORDER_QUERY.getDerivedColumnName(columnSegment.getIdentifier().getValue(), databaseType);
        result.add(new ColumnProjection(owner, new IdentifierValue(orderQueryName, quoteCharacter), null, databaseType));
    }
}
