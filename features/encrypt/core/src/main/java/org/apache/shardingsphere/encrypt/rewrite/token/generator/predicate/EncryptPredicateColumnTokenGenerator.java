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

package org.apache.shardingsphere.encrypt.rewrite.token.generator.predicate;

import com.cedarsoftware.util.CaseInsensitiveSet;
import com.google.common.base.Preconditions;
import com.sphereex.dbplusengine.SphereEx;
import com.sphereex.dbplusengine.SphereEx.Type;
import com.sphereex.dbplusengine.encrypt.rewrite.token.cryptographic.generator.function.EncryptFunctionSQLTokenGeneratorEngine;
import com.sphereex.dbplusengine.encrypt.rewrite.token.cryptographic.util.EncryptTokenGeneratorUtils;
import com.sphereex.dbplusengine.encrypt.rule.column.item.OrderQueryColumnItem;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import org.apache.shardingsphere.encrypt.enums.EncryptDerivedColumnSuffix;
import org.apache.shardingsphere.encrypt.exception.metadata.MissingMatchedEncryptQueryAlgorithmException;
import org.apache.shardingsphere.encrypt.rule.EncryptRule;
import org.apache.shardingsphere.encrypt.rule.column.EncryptColumn;
import org.apache.shardingsphere.encrypt.rule.column.item.LikeQueryColumnItem;
import org.apache.shardingsphere.encrypt.rule.table.EncryptTable;
import org.apache.shardingsphere.infra.annotation.HighFrequencyInvocation;
import org.apache.shardingsphere.infra.binder.context.extractor.SQLStatementContextExtractor;
import org.apache.shardingsphere.infra.binder.context.segment.select.projection.Projection;
import org.apache.shardingsphere.infra.binder.context.segment.select.projection.impl.ColumnProjection;
import org.apache.shardingsphere.infra.binder.context.segment.table.TablesContext;
import org.apache.shardingsphere.infra.binder.context.statement.SQLStatementContext;
import org.apache.shardingsphere.infra.binder.context.statement.dml.SelectStatementContext;
import org.apache.shardingsphere.infra.binder.context.type.WhereAvailable;
import org.apache.shardingsphere.infra.database.core.metadata.database.DialectDatabaseMetaData;
import org.apache.shardingsphere.infra.database.core.type.DatabaseType;
import org.apache.shardingsphere.infra.database.core.type.DatabaseTypeRegistry;
import org.apache.shardingsphere.infra.exception.core.ShardingSpherePreconditions;
import org.apache.shardingsphere.infra.metadata.ShardingSphereMetaData;
import org.apache.shardingsphere.infra.metadata.database.ShardingSphereDatabase;
import org.apache.shardingsphere.infra.rewrite.sql.token.common.generator.CollectionSQLTokenGenerator;
import org.apache.shardingsphere.infra.rewrite.sql.token.common.pojo.SQLToken;
import org.apache.shardingsphere.infra.rewrite.sql.token.common.pojo.generic.SubstitutableColumnNameToken;
import org.apache.shardingsphere.sql.parser.statement.core.enums.TableSourceType;
import org.apache.shardingsphere.sql.parser.statement.core.extractor.ColumnExtractor;
import org.apache.shardingsphere.sql.parser.statement.core.extractor.ExpressionExtractor;
import org.apache.shardingsphere.sql.parser.statement.core.segment.dml.column.ColumnSegment;
import org.apache.shardingsphere.sql.parser.statement.core.segment.dml.expr.BetweenExpression;
import org.apache.shardingsphere.sql.parser.statement.core.segment.dml.expr.BinaryOperationExpression;
import org.apache.shardingsphere.sql.parser.statement.core.segment.dml.expr.ExpressionSegment;
import org.apache.shardingsphere.sql.parser.statement.core.segment.dml.expr.FunctionSegment;
import org.apache.shardingsphere.sql.parser.statement.core.segment.dml.expr.simple.LiteralExpressionSegment;
import org.apache.shardingsphere.sql.parser.statement.core.segment.dml.expr.simple.ParameterMarkerExpressionSegment;
import org.apache.shardingsphere.sql.parser.statement.core.segment.dml.item.AggregationProjectionSegment;
import org.apache.shardingsphere.sql.parser.statement.core.segment.dml.item.ExpressionProjectionSegment;
import org.apache.shardingsphere.sql.parser.statement.core.segment.dml.predicate.AndPredicate;
import org.apache.shardingsphere.sql.parser.statement.core.segment.dml.predicate.HavingSegment;
import org.apache.shardingsphere.sql.parser.statement.core.segment.dml.predicate.WhereSegment;
import org.apache.shardingsphere.sql.parser.statement.core.value.identifier.IdentifierValue;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Optional;

/**
 * Predicate column token generator for encrypt.
 */
@HighFrequencyInvocation
@RequiredArgsConstructor
@Setter
public final class EncryptPredicateColumnTokenGenerator implements CollectionSQLTokenGenerator<SQLStatementContext> {
    
    @SphereEx
    private static final Collection<String> GREATER_LESS_COMPARISON_OPERATORS = new HashSet<>(Arrays.asList(">", "<", ">=", "<="));
    
    @SphereEx
    private static final Collection<String> SUPPORTED_AGGREGATION_FUNCTIONS = new CaseInsensitiveSet<>(Arrays.asList("MAX", "MIN"));
    
    private final EncryptRule rule;
    
    @SphereEx
    private final Map<String, EncryptRule> databaseEncryptRules;
    
    @SphereEx
    private final ShardingSphereDatabase database;
    
    @SphereEx
    private final ShardingSphereMetaData metaData;
    
    @Override
    public boolean isGenerateSQLToken(final SQLStatementContext sqlStatementContext) {
        return sqlStatementContext instanceof WhereAvailable;
    }
    
    @Override
    public Collection<SQLToken> generateSQLTokens(final SQLStatementContext sqlStatementContext) {
        Collection<SelectStatementContext> allSubqueryContexts = SQLStatementContextExtractor.getAllSubqueryContexts(sqlStatementContext);
        Collection<WhereSegment> whereSegments = SQLStatementContextExtractor.getWhereSegments((WhereAvailable) sqlStatementContext, allSubqueryContexts);
        // SPEX ADDED: BEGIN
        // NOTE: 加密投影列比较运算和 WHERE 条件中的比较运算改写逻辑一致，此处借用了 WHERE 中的比较运算的改写逻辑
        Collection<WhereSegment> projectionCompareOperatorWhereSegments =
                EncryptTokenGeneratorUtils.getProjectionCompareOperatorWhereSegments((WhereAvailable) sqlStatementContext, allSubqueryContexts);
        whereSegments.addAll(projectionCompareOperatorWhereSegments);
        // SPEX ADDED: END
        Collection<AndPredicate> andPredicates = getAndPredicates(whereSegments);
        return generateSQLTokens(andPredicates, sqlStatementContext);
    }
    
    private Collection<SQLToken> generateSQLTokens(final Collection<AndPredicate> andPredicates, final SQLStatementContext sqlStatementContext) {
        Collection<SQLToken> result = new LinkedList<>();
        @SphereEx
        boolean needRewriteUsingNaturalJoin = sqlStatementContext instanceof SelectStatementContext
                && EncryptTokenGeneratorUtils.isNeedRewriteUsingNaturalJoin((SelectStatementContext) sqlStatementContext, rule, databaseEncryptRules);
        for (AndPredicate each : andPredicates) {
            for (ExpressionSegment expression : each.getPredicates()) {
                // SPEX CHANGED: BEGIN
                result.addAll(generateSQLTokens(sqlStatementContext, expression, needRewriteUsingNaturalJoin));
                // SPEX CHANGED: END
            }
        }
        return result;
    }
    
    private Collection<SQLToken> generateSQLTokens(final SQLStatementContext sqlStatementContext, final ExpressionSegment expression, @SphereEx final boolean needRewriteUsingNaturalJoin) {
        Collection<SQLToken> result = new LinkedList<>();
        for (ColumnSegment each : ColumnExtractor.extract(expression)) {
            @SphereEx(Type.MODIFY)
            Optional<EncryptTable> encryptTable = getRule(each).findEncryptTable(each.getColumnBoundInfo().getOriginalTable().getValue());
            if (encryptTable.isPresent() && encryptTable.get().isEncryptColumn(each.getColumnBoundInfo().getOriginalColumn().getValue())) {
                EncryptColumn encryptColumn = encryptTable.get().getEncryptColumn(each.getColumnBoundInfo().getOriginalColumn().getValue());
                // SPEX CHANGED: BEGIN
                result.addAll(buildSubstitutableColumnNameTokens(encryptColumn, each, expression, sqlStatementContext.getDatabaseType(), encryptTable.get().getTable(), sqlStatementContext));
                // SPEX CHANGED: END
                // SPEX ADDED: BEGIN
            } else if (needRewriteUsingNaturalJoin && EncryptTokenGeneratorUtils.containsUsingColumn(each, ((SelectStatementContext) sqlStatementContext).getSqlStatement().getFrom().orElse(null))) {
                result.add(buildSubstitutableColumnNameToken(((SelectStatementContext) sqlStatementContext).getTablesContext(), each, sqlStatementContext.getDatabaseType()));
                // SPEX ADDED: END
            }
        }
        return result;
    }
    
    private Collection<AndPredicate> getAndPredicates(final Collection<WhereSegment> whereSegments) {
        Collection<AndPredicate> result = new LinkedList<>();
        for (WhereSegment each : whereSegments) {
            result.addAll(ExpressionExtractor.extractAndPredicates(each.getExpr()));
        }
        return result;
    }
    
    @SphereEx
    private EncryptRule getRule(final ColumnSegment columnSegment) {
        return databaseEncryptRules.getOrDefault(columnSegment.getColumnBoundInfo().getOriginalDatabase().getValue(), rule);
    }
    
    private Collection<SQLToken> buildSubstitutableColumnNameTokens(final EncryptColumn encryptColumn, final ColumnSegment columnSegment,
                                                                    final ExpressionSegment expression, final DatabaseType databaseType,
                                                                    @SphereEx final String tableName, @SphereEx final SQLStatementContext sqlStatementContext) {
        int startIndex = columnSegment.getOwner().isPresent() ? columnSegment.getOwner().get().getStopIndex() + 2 : columnSegment.getStartIndex();
        int stopIndex = columnSegment.getStopIndex();
        // SPEX ADDED: BEGIN
        if (encryptColumn.getPlain().isPresent() && getRule(columnSegment).isQueryWithPlain(tableName, encryptColumn.getName())) {
            Collection<Projection> columnProjections = createColumnProjections(encryptColumn.getPlain().get().getName(), columnSegment, EncryptDerivedColumnSuffix.PLAIN, databaseType, false);
            return Collections.singleton(new SubstitutableColumnNameToken(startIndex, stopIndex, columnProjections, databaseType, database, metaData));
        }
        // NOTE: 此场景中 columnSegment 由于是统一提取的，会出现 HAVING COUNT(LINK_WAY) > 1 中的列也被提取出来，此时如果该列也在 GROUP BY 中，改写结果 HAVING COUNT(MIN(LINK_WAY_CIPHER)) > 1 会导致语法异常
        boolean containsInGroupByItem = isNotIncludeInHavingFunctionExpression(columnSegment, sqlStatementContext)
                && EncryptTokenGeneratorUtils.isEncryptColumnContainsInGroupByItem(columnSegment, encryptColumn, sqlStatementContext);
        if (isIncludeGreaterLessOrBetween(expression)) {
            ShardingSpherePreconditions.checkState(encryptColumn.getOrderQuery().isPresent() || encryptColumn.getCipher().getEncryptor().getMetaData().isSupportOrder(),
                    () -> new MissingMatchedEncryptQueryAlgorithmException(tableName, columnSegment.getIdentifier().getValue(), "ORDER"));
            Collection<Projection> columnProjections = createColumnProjections(encryptColumn.getOrderQuery().map(OrderQueryColumnItem::getName)
                    .orElseGet(() -> encryptColumn.getCipher().getName()), columnSegment, EncryptDerivedColumnSuffix.ORDER_QUERY, databaseType, containsInGroupByItem);
            return Collections.singleton(new SubstitutableColumnNameToken(startIndex, stopIndex, columnProjections, databaseType, database, metaData));
        }
        if (isIncludeExpressionEvaluation(expression)) {
            return new EncryptFunctionSQLTokenGeneratorEngine(rule, databaseEncryptRules, database, metaData, database.getProtocolType())
                    .generateSQLTokens(new ExpressionProjectionSegment(startIndex, stopIndex, expression.getText(), expression));
        }
        // SPEX ADDED: END
        if (isIncludeLike(expression)) {
            Optional<LikeQueryColumnItem> likeQueryColumnItem = encryptColumn.getLikeQuery();
            Preconditions.checkState(likeQueryColumnItem.isPresent());
            // SPEX CHANGED: BEGIN
            return Collections.singleton(new SubstitutableColumnNameToken(startIndex, stopIndex, createColumnProjections(
                    likeQueryColumnItem.get().getName(), columnSegment, EncryptDerivedColumnSuffix.LIKE_QUERY, databaseType, containsInGroupByItem), databaseType, database, metaData));
        }
        Collection<Projection> columnProjections = encryptColumn.getAssistedQuery()
                .map(optional -> createColumnProjections(optional.getName(), columnSegment, EncryptDerivedColumnSuffix.ASSISTED_QUERY, databaseType, containsInGroupByItem))
                .orElseGet(() -> createColumnProjections(encryptColumn.getCipher().getName(), columnSegment, EncryptDerivedColumnSuffix.CIPHER, databaseType, containsInGroupByItem));
        return Collections.singleton(new SubstitutableColumnNameToken(startIndex, stopIndex, columnProjections, databaseType, database, metaData));
        // SPEX CHANGED: END
    }
    
    @SphereEx
    private SubstitutableColumnNameToken buildSubstitutableColumnNameToken(final TablesContext tablesContext, final ColumnSegment columnSegment, final DatabaseType databaseType) {
        int startIndex = columnSegment.getOwner().isPresent() ? columnSegment.getOwner().get().getStopIndex() + 2 : columnSegment.getStartIndex();
        int stopIndex = columnSegment.getStopIndex();
        IdentifierValue owner = tablesContext.getTableNameAliasMap().get(columnSegment.getColumnBoundInfo().getOriginalTable().getValue().toLowerCase());
        return new SubstitutableColumnNameToken(
                startIndex, stopIndex, Collections.singleton(new ColumnProjection(owner, columnSegment.getIdentifier(), null, databaseType)), databaseType, database, metaData);
    }
    
    @SphereEx
    private boolean isNotIncludeInHavingFunctionExpression(final ColumnSegment columnSegment, final SQLStatementContext sqlStatementContext) {
        if (sqlStatementContext instanceof SelectStatementContext && ((SelectStatementContext) sqlStatementContext).getSqlStatement().getHaving().isPresent()) {
            HavingSegment havingSegment = ((SelectStatementContext) sqlStatementContext).getSqlStatement().getHaving().get();
            if (!(havingSegment.getExpr() instanceof BinaryOperationExpression)) {
                return true;
            }
            BinaryOperationExpression binaryOperation = (BinaryOperationExpression) havingSegment.getExpr();
            if (binaryOperation.getLeft() instanceof AggregationProjectionSegment && isContainsColumnSegment(binaryOperation.getLeft(), columnSegment)) {
                return false;
            }
            return !(binaryOperation.getRight() instanceof AggregationProjectionSegment) || !isContainsColumnSegment(binaryOperation.getRight(), columnSegment);
        }
        return true;
    }
    
    @SphereEx
    private boolean isIncludeGreaterLessOrBetween(final ExpressionSegment expression) {
        return (expression instanceof BinaryOperationExpression || expression instanceof BetweenExpression) && isSupportedGreaterLessOrBetween(expression);
    }
    
    @SphereEx
    private boolean isSupportedGreaterLessOrBetween(final ExpressionSegment expressionSegment) {
        if (expressionSegment instanceof BinaryOperationExpression) {
            BinaryOperationExpression expression = (BinaryOperationExpression) expressionSegment;
            return GREATER_LESS_COMPARISON_OPERATORS.contains(expression.getOperator())
                    && isSupportedGreaterLessOrBetween(expression.getLeft()) && isSupportedGreaterLessOrBetween(expression.getRight());
        }
        if (expressionSegment instanceof BetweenExpression) {
            BetweenExpression expression = (BetweenExpression) expressionSegment;
            return isSupportedGreaterLessOrBetween(expression.getLeft())
                    && isSupportedGreaterLessOrBetween(expression.getBetweenExpr()) && isSupportedGreaterLessOrBetween(expression.getAndExpr());
        }
        if (expressionSegment instanceof AggregationProjectionSegment && SUPPORTED_AGGREGATION_FUNCTIONS.contains(((AggregationProjectionSegment) expressionSegment).getType().name())) {
            return true;
        }
        return expressionSegment instanceof ColumnSegment || expressionSegment instanceof LiteralExpressionSegment || expressionSegment instanceof ParameterMarkerExpressionSegment;
    }
    
    @SphereEx
    private boolean isIncludeExpressionEvaluation(final ExpressionSegment expression) {
        if (expression instanceof BinaryOperationExpression) {
            return isIncludeExpressionEvaluation(((BinaryOperationExpression) expression).getLeft()) || isIncludeExpressionEvaluation(((BinaryOperationExpression) expression).getRight());
        }
        return expression instanceof FunctionSegment;
    }
    
    private boolean isIncludeLike(final ExpressionSegment expression) {
        return expression instanceof BinaryOperationExpression && "LIKE".equalsIgnoreCase(((BinaryOperationExpression) expression).getOperator());
    }
    
    @SphereEx
    private boolean isContainsColumnSegment(final ExpressionSegment expressionSegment, final ColumnSegment targetColumnSegment) {
        return expressionSegment.getStartIndex() <= targetColumnSegment.getStartIndex() && expressionSegment.getStopIndex() >= targetColumnSegment.getStopIndex();
    }
    
    private Collection<Projection> createColumnProjections(final String actualColumnName, final ColumnSegment columnSegment, final EncryptDerivedColumnSuffix derivedColumnSuffix,
                                                           final DatabaseType databaseType, @SphereEx final boolean encryptColumnContainsInGroupByItem) {
        String columnName = TableSourceType.TEMPORARY_TABLE == columnSegment.getColumnBoundInfo().getTableSourceType()
                ? derivedColumnSuffix.getDerivedColumnName(columnSegment.getIdentifier().getValue(), databaseType)
                : actualColumnName;
        DialectDatabaseMetaData dialectDatabaseMetaData = new DatabaseTypeRegistry(databaseType).getDialectDatabaseMetaData();
        ColumnProjection columnProjection = new ColumnProjection(null, new IdentifierValue(columnName, dialectDatabaseMetaData.getQuoteCharacter()), null, databaseType);
        // SPEX ADDED: BEGIN
        columnProjection.setEncryptColumnContainsInGroupByItem(encryptColumnContainsInGroupByItem);
        // SPEX ADDED: END
        return Collections.singleton(columnProjection);
    }
}
