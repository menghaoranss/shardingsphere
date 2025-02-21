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

package org.apache.shardingsphere.encrypt.rewrite.token.generator.projection;

import com.cedarsoftware.util.CaseInsensitiveSet;
import com.sphereex.dbplusengine.SphereEx;
import com.sphereex.dbplusengine.SphereEx.Type;
import com.sphereex.dbplusengine.encrypt.checker.cryptographic.CombineProjectionColumnsEncryptorChecker;
import com.sphereex.dbplusengine.encrypt.rewrite.token.cryptographic.generator.function.EncryptFunctionSQLTokenGeneratorEngine;
import com.sphereex.dbplusengine.encrypt.rewrite.token.cryptographic.util.EncryptTokenGeneratorUtils;
import com.sphereex.dbplusengine.infra.util.DatabaseTypeUtils;
import lombok.RequiredArgsConstructor;
import org.apache.shardingsphere.encrypt.rule.EncryptRule;
import org.apache.shardingsphere.encrypt.rule.column.EncryptColumn;
import org.apache.shardingsphere.encrypt.rule.table.EncryptTable;
import org.apache.shardingsphere.infra.annotation.HighFrequencyInvocation;
import org.apache.shardingsphere.infra.binder.context.segment.select.projection.DerivedColumn;
import org.apache.shardingsphere.infra.binder.context.segment.select.projection.Projection;
import org.apache.shardingsphere.infra.binder.context.segment.select.projection.ProjectionsContext;
import org.apache.shardingsphere.infra.binder.context.segment.select.projection.impl.ColumnProjection;
import org.apache.shardingsphere.infra.binder.context.segment.select.projection.impl.ShorthandProjection;
import org.apache.shardingsphere.infra.binder.context.statement.dml.SelectStatementContext;
import org.apache.shardingsphere.infra.database.core.metadata.database.enums.QuoteCharacter;
import org.apache.shardingsphere.infra.database.core.type.DatabaseType;
import org.apache.shardingsphere.infra.exception.generic.UnsupportedSQLOperationException;
import org.apache.shardingsphere.infra.metadata.ShardingSphereMetaData;
import org.apache.shardingsphere.infra.metadata.database.ShardingSphereDatabase;
import org.apache.shardingsphere.infra.rewrite.sql.token.common.pojo.SQLToken;
import org.apache.shardingsphere.infra.rewrite.sql.token.common.pojo.generic.SubstitutableColumnNameToken;
import org.apache.shardingsphere.sql.parser.statement.core.enums.SubqueryType;
import org.apache.shardingsphere.sql.parser.statement.core.segment.dml.column.ColumnSegment;
import org.apache.shardingsphere.sql.parser.statement.core.segment.dml.item.ColumnProjectionSegment;
import org.apache.shardingsphere.sql.parser.statement.core.segment.dml.item.ProjectionSegment;
import org.apache.shardingsphere.sql.parser.statement.core.segment.dml.item.ShorthandProjectionSegment;
import org.apache.shardingsphere.sql.parser.statement.core.segment.generic.OwnerSegment;
import org.apache.shardingsphere.sql.parser.statement.core.segment.generic.ParenthesesSegment;
import org.apache.shardingsphere.sql.parser.statement.core.segment.generic.bound.ColumnSegmentBoundInfo;
import org.apache.shardingsphere.sql.parser.statement.core.segment.generic.table.TableSegment;
import org.apache.shardingsphere.sql.parser.statement.core.value.identifier.IdentifierValue;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Projection token generator for encrypt.
 */
@HighFrequencyInvocation
@RequiredArgsConstructor
public final class EncryptProjectionTokenGenerator {
    
    private final List<SQLToken> previousSQLTokens;
    
    private final DatabaseType databaseType;
    
    private final EncryptRule rule;
    
    @SphereEx
    private final Map<String, EncryptRule> databaseEncryptRules;
    
    @SphereEx
    private final ShardingSphereDatabase database;
    
    @SphereEx
    private final ShardingSphereMetaData metaData;
    
    /**
     * Generate SQL tokens.
     *
     * @param selectStatementContext select statement context
     * @return generated SQL tokens
     */
    public Collection<SQLToken> generateSQLTokens(final SelectStatementContext selectStatementContext) {
        Collection<SQLToken> result = new LinkedList<>();
        selectStatementContext.getSubqueryContexts().values().stream().map(this::generateSQLTokens).forEach(result::addAll);
        result.addAll(generateSelectSQLTokens(selectStatementContext));
        return result;
    }
    
    private Collection<SQLToken> generateSelectSQLTokens(final SelectStatementContext selectStatementContext) {
        // SPEX ADDED: BEGIN
        CombineProjectionColumnsEncryptorChecker.checkIsSame(selectStatementContext, rule, databaseEncryptRules);
        // SPEX ADDED: END
        Collection<SQLToken> result = new LinkedList<>();
        Collection<String> existColumnNames = new CaseInsensitiveSet<>();
        @SphereEx
        boolean needRewriteUsingNaturalJoin = EncryptTokenGeneratorUtils.isNeedRewriteUsingNaturalJoin(selectStatementContext, rule, databaseEncryptRules);
        for (ProjectionSegment each : selectStatementContext.getSqlStatement().getProjections().getProjections()) {
            if (each instanceof ColumnProjectionSegment) {
                generateSQLToken(selectStatementContext, (ColumnProjectionSegment) each, existColumnNames).ifPresent(result::add);
            }
            if (each instanceof ShorthandProjectionSegment) {
                ShorthandProjectionSegment shorthandSegment = (ShorthandProjectionSegment) each;
                Collection<Projection> actualColumns = getShorthandProjection(shorthandSegment, selectStatementContext.getProjectionsContext()).getActualColumns();
                if (!actualColumns.isEmpty()) {
                    // SPEX CHANGED: BEGIN
                    result.add(generateSQLToken(shorthandSegment, actualColumns, selectStatementContext, selectStatementContext.getSubqueryType(), existColumnNames, needRewriteUsingNaturalJoin));
                    // SPEX CHANGED: END
                }
            }
            // SPEX ADDED: BEGIN
            result.addAll(new EncryptFunctionSQLTokenGeneratorEngine(rule, databaseEncryptRules, database, metaData, databaseType).generateSQLTokens(each));
            // SPEX ADDED: END
        }
        return result;
    }
    
    private Optional<SubstitutableColumnNameToken> generateSQLToken(final SelectStatementContext selectStatementContext, final ColumnProjectionSegment columnSegment,
                                                                    final Collection<String> existColumnNames) {
        ColumnProjection columnProjection = buildColumnProjection(columnSegment);
        String columnName = columnProjection.getOriginalColumn().getValue();
        boolean newAddedColumn = existColumnNames.add(columnProjection.getOriginalTable().getValue() + "." + columnName);
        @SphereEx(Type.MODIFY)
        Optional<EncryptTable> encryptTable = getRule(columnSegment.getColumn().getColumnBoundInfo()).findEncryptTable(columnProjection.getOriginalTable().getValue());
        @SphereEx
        boolean isInWith = selectStatementContext.getSqlStatement().isInWith();
        if (encryptTable.isPresent() && encryptTable.get().isEncryptColumn(columnName)) {
            EncryptColumn encryptColumn = encryptTable.get().getEncryptColumn(columnName);
            @SphereEx(Type.MODIFY)
            Collection<Projection> projections = generateProjections(encryptColumn, columnProjection, selectStatementContext.getSubqueryType(), newAddedColumn, isInWith);
            int startIndex = getStartIndex(columnSegment);
            int stopIndex = getStopIndex(columnSegment);
            previousSQLTokens.removeIf(each -> each.getStartIndex() == startIndex);
            // SPEX CHANGED: BEGIN
            return Optional.of(new SubstitutableColumnNameToken(startIndex, stopIndex, projections, databaseType, database, metaData, false));
            // SPEX CHANGED: END
        }
        return Optional.empty();
    }
    
    private SubstitutableColumnNameToken generateSQLToken(final ShorthandProjectionSegment segment, final Collection<Projection> actualColumns, final SelectStatementContext selectStatementContext,
                                                          final SubqueryType subqueryType, final Collection<String> existColumnNames, @SphereEx final boolean needRewriteUsingNaturalJoin) {
        Collection<Projection> projections = new LinkedList<>();
        for (Projection each : actualColumns) {
            @SphereEx
            Projection projection = removeUsingColumnOwner(each, needRewriteUsingNaturalJoin, selectStatementContext.getSqlStatement().getFrom().orElse(null));
            if (each instanceof ColumnProjection) {
                ColumnProjection columnProjection = (ColumnProjection) each;
                boolean newAddedColumn = existColumnNames.add(columnProjection.getOriginalTable().getValue() + "." + columnProjection.getOriginalColumn().getValue());
                @SphereEx
                boolean isInWith = selectStatementContext.getSqlStatement().isInWith();
                @SphereEx(Type.MODIFY)
                Optional<EncryptTable> encryptTable = getRule(columnProjection.getColumnBoundInfo()).findEncryptTable(columnProjection.getOriginalTable().getValue());
                if (encryptTable.isPresent() && encryptTable.get().isEncryptColumn(columnProjection.getOriginalColumn().getValue())) {
                    EncryptColumn encryptColumn = encryptTable.get().getEncryptColumn(columnProjection.getOriginalColumn().getValue());
                    // SPEX CHANGED: BEGIN
                    projections.addAll(generateProjections(encryptColumn, columnProjection, subqueryType, newAddedColumn, isInWith));
                    // SPEX CHANGED: END
                    continue;
                }
            }
            // SPEX CHANGED: BEGIN
            projections.add(projection.getAlias().filter(alias -> !DerivedColumn.isDerivedColumnName(alias.getValue()))
                    .map(optional -> (Projection) new ColumnProjection(null, optional, null, databaseType)).orElse(projection));
            // SPEX CHANGED: END
        }
        int startIndex = segment.getOwner().isPresent() ? segment.getOwner().get().getStartIndex() : segment.getStartIndex();
        previousSQLTokens.removeIf(each -> each.getStartIndex() == startIndex);
        // SPEX CHANGED: BEGIN
        String databaseName = actualColumns.stream().filter(each -> each instanceof ColumnProjection)
                .map(optional -> ((ColumnProjection) optional).getColumnBoundInfo().getOriginalDatabase().getValue()).findFirst().orElse(database.getName());
        ShardingSphereDatabase usedDatabase = metaData.containsDatabase(databaseName) ? metaData.getDatabase(databaseName) : database;
        return new SubstitutableColumnNameToken(startIndex, segment.getStopIndex(), projections, selectStatementContext.getDatabaseType(), usedDatabase, metaData, true);
        // SPEX CHANGED: END
    }
    
    @SphereEx
    private EncryptRule getRule(final ColumnSegmentBoundInfo columnBoundInfo) {
        return databaseEncryptRules.getOrDefault(columnBoundInfo.getOriginalDatabase().getValue(), rule);
    }
    
    private int getStartIndex(final ColumnProjectionSegment columnSegment) {
        if (columnSegment.getColumn().getLeftParentheses().isPresent()) {
            return columnSegment.getColumn().getLeftParentheses().get().getStartIndex();
        }
        return columnSegment.getColumn().getOwner().isPresent() ? columnSegment.getColumn().getOwner().get().getStartIndex() : columnSegment.getColumn().getStartIndex();
    }
    
    private int getStopIndex(final ColumnProjectionSegment columnSegment) {
        if (columnSegment.getAliasSegment().isPresent()) {
            return columnSegment.getAliasSegment().get().getStopIndex();
        }
        return columnSegment.getColumn().getRightParentheses().isPresent() ? columnSegment.getColumn().getRightParentheses().get().getStopIndex() : columnSegment.getColumn().getStopIndex();
    }
    
    private ColumnProjection buildColumnProjection(final ColumnProjectionSegment segment) {
        IdentifierValue owner = segment.getColumn().getOwner().map(OwnerSegment::getIdentifier).orElse(null);
        return new ColumnProjection(owner, segment.getColumn().getIdentifier(), segment.getAliasName().isPresent() ? segment.getAlias().orElse(null) : null, databaseType,
                segment.getColumn().getLeftParentheses().orElse(null), segment.getColumn().getRightParentheses().orElse(null), segment.getColumn().getColumnBoundInfo());
    }
    
    @SphereEx
    private Projection removeUsingColumnOwner(final Projection projection, final boolean needRewriteUsingNaturalJoin, final TableSegment tableSegment) {
        if (projection instanceof ColumnProjection && DatabaseTypeUtils.isOracleDatabase(databaseType) && !needRewriteUsingNaturalJoin
                && EncryptTokenGeneratorUtils.containsUsingColumn(new ColumnSegment(0, 0, ((ColumnProjection) projection).getName()), tableSegment)) {
            ColumnProjection columnProjection = (ColumnProjection) projection;
            return new ColumnProjection(null, columnProjection.getName(), projection.getAlias().orElse(null), columnProjection.getDatabaseType(), null, null, columnProjection.getColumnBoundInfo());
        }
        return projection;
    }
    
    private Collection<Projection> generateProjections(final EncryptColumn encryptColumn, final ColumnProjection columnProjection,
                                                       final SubqueryType subqueryType, final boolean newAddedColumn, @SphereEx final boolean isInWith) {
        // SPEX ADDED: BEGIN
        if (isInWith) {
            return generateProjectionsInTableSegmentSubquery(encryptColumn, columnProjection, newAddedColumn);
        }
        // SPEX ADDED: END
        if (null == subqueryType || SubqueryType.PROJECTION == subqueryType) {
            return Collections.singleton(generateProjection(encryptColumn, columnProjection));
        }
        if (SubqueryType.TABLE == subqueryType || SubqueryType.JOIN == subqueryType || SubqueryType.WITH == subqueryType) {
            return generateProjectionsInTableSegmentSubquery(encryptColumn, columnProjection, newAddedColumn);
        }
        if (SubqueryType.PREDICATE == subqueryType) {
            return Collections.singleton(generateProjectionInPredicateSubquery(encryptColumn, columnProjection));
        }
        if (SubqueryType.INSERT_SELECT == subqueryType || SubqueryType.VIEW_DEFINITION == subqueryType) {
            return generateProjectionsInInsertSelectSubquery(encryptColumn, columnProjection);
        }
        throw new UnsupportedSQLOperationException(
                "Projections not in simple select, table subquery, join subquery, predicate subquery and insert select subquery are not supported in encrypt feature.");
    }
    
    private ColumnProjection generateProjection(final EncryptColumn encryptColumn, final ColumnProjection columnProjection) {
        @SphereEx(Type.MODIFY)
        IdentifierValue cipherColumnName = new IdentifierValue(getEncryptColumnName(getRule(columnProjection.getColumnBoundInfo()), columnProjection, encryptColumn),
                columnProjection.getName().getQuoteCharacter());
        return new ColumnProjection(columnProjection.getOwner().orElse(null), cipherColumnName, columnProjection.getAlias().orElse(columnProjection.getName()), databaseType,
                columnProjection.getLeftParentheses().orElse(null), columnProjection.getRightParentheses().orElse(null));
    }
    
    @SphereEx
    private String getEncryptColumnName(final EncryptRule encryptRule, final ColumnProjection columnProjection, final EncryptColumn encryptColumn) {
        return encryptColumn.getPlain().isPresent() && encryptRule.isQueryWithPlain(columnProjection.getOriginalTable().getValue(), encryptColumn.getName())
                ? getPlainName(columnProjection.getName().getValue(), encryptColumn.getName(), encryptColumn.getPlain().get().getName())
                : encryptColumn.getCipher().getName();
    }
    
    private Collection<Projection> generateProjectionsInTableSegmentSubquery(final EncryptColumn encryptColumn, final ColumnProjection columnProjection, final boolean newAddedColumn) {
        Collection<Projection> result = new LinkedList<>();
        QuoteCharacter quoteCharacter = columnProjection.getName().getQuoteCharacter();
        IdentifierValue alias = columnProjection.getAlias().orElse(columnProjection.getName());
        // SPEX ADDED: BEGIN
        boolean queryWithPlain = encryptColumn.getPlain().isPresent()
                && getRule(columnProjection.getColumnBoundInfo()).isQueryWithPlain(columnProjection.getOriginalTable().getValue(), columnProjection.getOriginalColumn().getValue());
        if (queryWithPlain) {
            String plainName = getPlainName(columnProjection.getColumnName(), encryptColumn.getName(), encryptColumn.getPlain().get().getName());
            result.add(new ColumnProjection(columnProjection.getOwner().orElse(null), new IdentifierValue(plainName, quoteCharacter), alias, databaseType));
            appendPlainColumnWithoutAlias(result, columnProjection, new IdentifierValue(plainName, quoteCharacter), alias, newAddedColumn);
            return result;
        }
        // SPEX ADDED: END
        IdentifierValue cipherColumnName = new IdentifierValue(encryptColumn.getCipher().getName(), quoteCharacter);
        ParenthesesSegment leftParentheses = columnProjection.getLeftParentheses().orElse(null);
        ParenthesesSegment rightParentheses = columnProjection.getRightParentheses().orElse(null);
        result.add(new ColumnProjection(columnProjection.getOwner().orElse(null), cipherColumnName, alias, databaseType, leftParentheses, rightParentheses));
        if (newAddedColumn) {
            result.add(new ColumnProjection(columnProjection.getOwner().orElse(null), cipherColumnName, null, databaseType));
            IdentifierValue assistedColumOwner = columnProjection.getOwner().orElse(null);
            encryptColumn.getAssistedQuery().ifPresent(
                    optional -> result.add(new ColumnProjection(assistedColumOwner, new IdentifierValue(optional.getName(), quoteCharacter), null, databaseType, leftParentheses, rightParentheses)));
            encryptColumn.getLikeQuery().ifPresent(
                    optional -> result.add(new ColumnProjection(assistedColumOwner, new IdentifierValue(optional.getName(), quoteCharacter), null, databaseType, leftParentheses, rightParentheses)));
            // SPEX ADDED: BEGIN
            encryptColumn.getOrderQuery().ifPresent(optional -> result.add(new ColumnProjection(assistedColumOwner, new IdentifierValue(optional.getName(), quoteCharacter), null, databaseType)));
            // SPEX ADDED: END
        }
        return result;
    }
    
    @SphereEx
    private void appendPlainColumnWithoutAlias(final Collection<Projection> result, final ColumnProjection columnProjection, final IdentifierValue plainName, final IdentifierValue alias,
                                               final boolean newAddedColumn) {
        // NOTE: 明文列和逻辑列不同，明文列和别名不同，则追加一个明文列，用于外层查询引用
        if (newAddedColumn && !plainName.getValue().equalsIgnoreCase(columnProjection.getName().getValue()) && !plainName.getValue().equalsIgnoreCase(alias.getValue())) {
            result.add(new ColumnProjection(columnProjection.getOwner().orElse(null), plainName, null, databaseType));
        }
    }
    
    @SphereEx
    private String getPlainName(final String usedColumnName, final String logicColumnName, final String plainColumnName) {
        // NOTE: 如果当前查询列名和逻辑列名相同，说明需要转换为 PLAIN 列（PLAIN 列可以配置和逻辑列名不同），否则说明当前查询列，引用的是子查询 AS 别名列
        return usedColumnName.equalsIgnoreCase(logicColumnName) ? plainColumnName : usedColumnName;
    }
    
    private ColumnProjection generateProjectionInPredicateSubquery(final EncryptColumn encryptColumn, final ColumnProjection columnProjection) {
        QuoteCharacter quoteCharacter = columnProjection.getName().getQuoteCharacter();
        // SPEX ADDED: BEGIN
        if (encryptColumn.getPlain().isPresent()
                && getRule(columnProjection.getColumnBoundInfo()).isQueryWithPlain(columnProjection.getOriginalTable().getValue(), columnProjection.getOriginalColumn().getValue())) {
            String plainName = getPlainName(columnProjection.getColumnName(), encryptColumn.getName(), encryptColumn.getPlain().get().getName());
            return new ColumnProjection(columnProjection.getOwner().orElse(null), new IdentifierValue(plainName, quoteCharacter), null, databaseType);
        }
        // SPEX ADDED: END
        ParenthesesSegment leftParentheses = columnProjection.getLeftParentheses().orElse(null);
        ParenthesesSegment rightParentheses = columnProjection.getRightParentheses().orElse(null);
        IdentifierValue owner = columnProjection.getOwner().orElse(null);
        return encryptColumn.getAssistedQuery()
                .map(optional -> new ColumnProjection(owner, new IdentifierValue(optional.getName(), quoteCharacter), null, databaseType, leftParentheses, rightParentheses))
                .orElseGet(() -> new ColumnProjection(owner, new IdentifierValue(encryptColumn.getCipher().getName(), quoteCharacter), columnProjection.getAlias().orElse(columnProjection.getName()),
                        databaseType, leftParentheses, rightParentheses));
    }
    
    private Collection<Projection> generateProjectionsInInsertSelectSubquery(final EncryptColumn encryptColumn, final ColumnProjection columnProjection) {
        QuoteCharacter quoteCharacter = columnProjection.getName().getQuoteCharacter();
        IdentifierValue columnName = new IdentifierValue(encryptColumn.getCipher().getName(), quoteCharacter);
        Collection<Projection> result = new LinkedList<>();
        ParenthesesSegment leftParentheses = columnProjection.getLeftParentheses().orElse(null);
        ParenthesesSegment rightParentheses = columnProjection.getRightParentheses().orElse(null);
        result.add(new ColumnProjection(columnProjection.getOwner().orElse(null), columnName, null, databaseType, leftParentheses, rightParentheses));
        IdentifierValue columOwner = columnProjection.getOwner().orElse(null);
        encryptColumn.getAssistedQuery()
                .ifPresent(optional -> result.add(new ColumnProjection(columOwner, new IdentifierValue(optional.getName(), quoteCharacter), null, databaseType, leftParentheses, rightParentheses)));
        encryptColumn.getLikeQuery()
                .ifPresent(optional -> result.add(new ColumnProjection(columOwner, new IdentifierValue(optional.getName(), quoteCharacter), null, databaseType, leftParentheses, rightParentheses)));
        // SPEX ADDED: BEGIN
        encryptColumn.getOrderQuery().ifPresent(optional -> result.add(new ColumnProjection(columOwner, new IdentifierValue(optional.getName(), quoteCharacter), null, databaseType)));
        encryptColumn.getPlain()
                .ifPresent(optional -> result.add(new ColumnProjection(columOwner, new IdentifierValue(optional.getName(), quoteCharacter), null, databaseType)));
        // SPEX ADDED: END
        return result;
    }
    
    private ShorthandProjection getShorthandProjection(final ShorthandProjectionSegment segment, final ProjectionsContext projectionsContext) {
        Optional<String> owner = segment.getOwner().isPresent() ? Optional.of(segment.getOwner().get().getIdentifier().getValue()) : Optional.empty();
        for (Projection each : projectionsContext.getProjections()) {
            if (each instanceof ShorthandProjection) {
                if (!owner.isPresent() && !((ShorthandProjection) each).getOwner().isPresent()) {
                    return (ShorthandProjection) each;
                }
                if (owner.isPresent() && owner.get().equals(((ShorthandProjection) each).getOwner().map(IdentifierValue::getValue).orElse(null))) {
                    return (ShorthandProjection) each;
                }
            }
        }
        throw new IllegalStateException(String.format("Can not find shorthand projection segment, owner is `%s`", owner.orElse(null)));
    }
}
