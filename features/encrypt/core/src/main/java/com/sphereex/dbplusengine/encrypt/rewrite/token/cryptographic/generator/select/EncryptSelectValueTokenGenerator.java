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

package com.sphereex.dbplusengine.encrypt.rewrite.token.cryptographic.generator.select;

import com.cedarsoftware.util.CaseInsensitiveSet;
import com.sphereex.dbplusengine.encrypt.rewrite.token.cryptographic.pojo.EncryptSelectValueToken;
import com.sphereex.dbplusengine.encrypt.rule.column.item.OrderQueryColumnItem;
import com.sphereex.dbplusengine.encrypt.rule.column.item.PlainColumnItem;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import org.apache.shardingsphere.encrypt.enums.EncryptDerivedColumnSuffix;
import org.apache.shardingsphere.encrypt.rule.EncryptRule;
import org.apache.shardingsphere.encrypt.rule.column.EncryptColumn;
import org.apache.shardingsphere.encrypt.rule.column.item.AssistedQueryColumnItem;
import org.apache.shardingsphere.encrypt.rule.column.item.LikeQueryColumnItem;
import org.apache.shardingsphere.encrypt.rule.table.EncryptTable;
import org.apache.shardingsphere.infra.binder.context.statement.SQLStatementContext;
import org.apache.shardingsphere.infra.binder.context.statement.dml.SelectStatementContext;
import org.apache.shardingsphere.infra.database.core.type.DatabaseType;
import org.apache.shardingsphere.infra.database.core.type.DatabaseTypeRegistry;
import org.apache.shardingsphere.infra.metadata.database.ShardingSphereDatabase;
import org.apache.shardingsphere.infra.rewrite.sql.token.common.generator.CollectionSQLTokenGenerator;
import org.apache.shardingsphere.infra.rewrite.sql.token.common.pojo.SQLToken;
import org.apache.shardingsphere.sql.parser.statement.core.enums.SubqueryType;
import org.apache.shardingsphere.sql.parser.statement.core.segment.dml.combine.CombineSegment;
import org.apache.shardingsphere.sql.parser.statement.core.segment.dml.expr.ExpressionSegment;
import org.apache.shardingsphere.sql.parser.statement.core.segment.dml.expr.simple.LiteralExpressionSegment;
import org.apache.shardingsphere.sql.parser.statement.core.segment.dml.expr.simple.ParameterMarkerExpressionSegment;
import org.apache.shardingsphere.sql.parser.statement.core.segment.dml.item.ColumnProjectionSegment;
import org.apache.shardingsphere.sql.parser.statement.core.segment.dml.item.ExpressionProjectionSegment;
import org.apache.shardingsphere.sql.parser.statement.core.segment.dml.item.ProjectionSegment;
import org.apache.shardingsphere.sql.parser.statement.core.segment.generic.bound.ColumnSegmentBoundInfo;

import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Select value token generator for encrypt.
 */
@RequiredArgsConstructor
@Setter
public final class EncryptSelectValueTokenGenerator implements CollectionSQLTokenGenerator<SelectStatementContext> {
    
    private static final Collection<SubqueryType> NEED_REWRITTEN_SUBQUERY_TYPES = new CaseInsensitiveSet<>(Arrays.asList(SubqueryType.TABLE, SubqueryType.JOIN, SubqueryType.WITH));
    
    private final EncryptRule rule;
    
    private final Map<String, EncryptRule> databaseEncryptRules;
    
    private final ShardingSphereDatabase database;
    
    @Override
    public boolean isGenerateSQLToken(final SQLStatementContext sqlStatementContext) {
        return sqlStatementContext instanceof SelectStatementContext;
    }
    
    @Override
    public Collection<SQLToken> generateSQLTokens(final SelectStatementContext selectStatementContext) {
        Collection<SQLToken> result = new LinkedList<>();
        if (selectStatementContext.getSqlStatement().getCombine().isPresent()) {
            generateSQLTokens(selectStatementContext, result);
        }
        selectStatementContext.getSubqueryContexts().values().forEach(each -> result.addAll(generateSQLTokens(each)));
        return result;
    }
    
    private void generateSQLTokens(final SelectStatementContext selectStatementContext, final Collection<SQLToken> result) {
        CombineSegment combineSegment = selectStatementContext.getSqlStatement().getCombine().orElseThrow(() -> new IllegalStateException("Can not get combine segment"));
        List<List<ProjectionSegment>> combineProjections = new LinkedList<>();
        extractProjectionsInCombineSegment(selectStatementContext, combineSegment, combineProjections);
        for (int i = 0; i < combineProjections.iterator().next().size(); i++) {
            generateSQLTokens(combineProjections, i, selectStatementContext, result);
        }
    }
    
    private void generateSQLTokens(final List<List<ProjectionSegment>> combineProjections, final int index, final SelectStatementContext selectStatementContext, final Collection<SQLToken> result) {
        Optional<EncryptColumn> encryptColumn = Optional.empty();
        ColumnProjectionSegment encryptColumnProjection = null;
        List<List<ProjectionSegment>> literalParameterMarkerProjections = new LinkedList<>();
        for (List<ProjectionSegment> each : combineProjections) {
            if (each.size() > index && each.get(index) instanceof ColumnProjectionSegment) {
                encryptColumn = findEncryptColumn(each.get(index));
                encryptColumnProjection = (ColumnProjectionSegment) each.get(index);
            }
            if (isLiteralOrParameterMarkerExpression(each.get(index))) {
                literalParameterMarkerProjections.add(each);
            }
        }
        if (encryptColumn.isPresent() && !literalParameterMarkerProjections.isEmpty()) {
            generateSQLTokens(encryptColumnProjection, index, selectStatementContext, literalParameterMarkerProjections, encryptColumn.get(), result);
        }
    }
    
    private void generateSQLTokens(final ColumnProjectionSegment encryptColumnProjection, final int index, final SelectStatementContext selectStatementContext,
                                   final List<List<ProjectionSegment>> literalParameterMarkerProjections, final EncryptColumn encryptColumn, final Collection<SQLToken> result) {
        String tableName = encryptColumnProjection.getColumn().getColumnBoundInfo().getOriginalTable().getValue();
        for (List<ProjectionSegment> each : literalParameterMarkerProjections) {
            ProjectionSegment projectionSegment = each.get(index);
            EncryptSelectValueToken literalToken = new EncryptSelectValueToken(projectionSegment.getStartIndex(), projectionSegment.getStopIndex());
            String schemaName = selectStatementContext.getTablesContext().getSchemaName()
                    .orElseGet(() -> new DatabaseTypeRegistry(selectStatementContext.getDatabaseType()).getDefaultSchemaName(database.getName()));
            generateSQLTokens(literalToken, schemaName, projectionSegment, encryptColumn, tableName, selectStatementContext, encryptColumnProjection.getColumnLabel());
            result.removeIf(sqlToken -> sqlToken.getStartIndex() == literalToken.getStartIndex());
            result.add(literalToken);
        }
    }
    
    private void generateSQLTokens(final EncryptSelectValueToken literalToken, final String schemaName, final ProjectionSegment projectionSegment,
                                   final EncryptColumn encryptColumn, final String tableName, final SelectStatementContext selectStatementContext, final String columnLabel) {
        DatabaseType databaseType = selectStatementContext.getDatabaseType();
        if (encryptColumn.getPlain().map(PlainColumnItem::isQueryWithPlain).orElse(false)) {
            addPlainColumn(projectionSegment, literalToken, columnLabel, databaseType);
            return;
        }
        SubqueryType subqueryType = selectStatementContext.getSubqueryType();
        addCipherColumn(schemaName, tableName, encryptColumn, literalToken, projectionSegment, columnLabel, databaseType);
        if (selectStatementContext.getSqlStatement().isInWith() || NEED_REWRITTEN_SUBQUERY_TYPES.contains(subqueryType)) {
            encryptColumn.getAssistedQuery().ifPresent(optional -> addAssistedQueryColumn(
                    schemaName, tableName, encryptColumn.getName(), literalToken, projectionSegment, optional, columnLabel, databaseType));
            encryptColumn.getLikeQuery().ifPresent(optional -> addLikeQueryColumn(
                    schemaName, tableName, encryptColumn.getName(), literalToken, projectionSegment, optional, columnLabel, databaseType));
            encryptColumn.getOrderQuery().ifPresent(optional -> addOrderQueryColumn(
                    schemaName, tableName, encryptColumn.getName(), literalToken, projectionSegment, optional, columnLabel, databaseType));
        }
    }
    
    private Optional<EncryptColumn> findEncryptColumn(final ProjectionSegment projectionSegment) {
        ColumnSegmentBoundInfo columnBoundInfo = ((ColumnProjectionSegment) projectionSegment).getColumn().getColumnBoundInfo();
        Optional<EncryptTable> encryptTable = databaseEncryptRules.getOrDefault(columnBoundInfo.getOriginalDatabase().getValue(), rule).findEncryptTable(columnBoundInfo.getOriginalTable().getValue());
        if (encryptTable.isPresent() && encryptTable.get().isEncryptColumn(columnBoundInfo.getOriginalColumn().getValue())) {
            return Optional.of(encryptTable.get().getEncryptColumn(columnBoundInfo.getOriginalColumn().getValue()));
        }
        return Optional.empty();
    }
    
    private boolean isLiteralOrParameterMarkerExpression(final ProjectionSegment projection) {
        if (projection instanceof ExpressionProjectionSegment) {
            return ((ExpressionProjectionSegment) projection).getExpr() instanceof LiteralExpressionSegment;
        }
        return projection instanceof ParameterMarkerExpressionSegment;
    }
    
    private static void extractProjectionsInCombineSegment(final SelectStatementContext selectStatementContext, final CombineSegment combineSegment,
                                                           final List<List<ProjectionSegment>> combineProjections) {
        extractProjectionsInCombineSegment(selectStatementContext.getSubqueryContexts().get(combineSegment.getLeft().getStartIndex()), combineProjections);
        extractProjectionsInCombineSegment(selectStatementContext.getSubqueryContexts().get(combineSegment.getRight().getStartIndex()), combineProjections);
    }
    
    private static void extractProjectionsInCombineSegment(final SelectStatementContext selectStatementContext, final List<List<ProjectionSegment>> projectionsInCombines) {
        if (selectStatementContext.getSqlStatement().getCombine().isPresent()) {
            extractProjectionsInCombineSegment(selectStatementContext, selectStatementContext.getSqlStatement().getCombine().get(), projectionsInCombines);
        } else {
            projectionsInCombines.add(selectStatementContext.getSqlStatement().getProjections().getProjections());
        }
    }
    
    private void addPlainColumn(final ProjectionSegment projectionSegment, final EncryptSelectValueToken literalToken, final String columnLabel, final DatabaseType databaseType) {
        String alias = EncryptDerivedColumnSuffix.PLAIN.getDerivedColumnName(columnLabel, databaseType);
        if (projectionSegment instanceof ExpressionProjectionSegment) {
            ExpressionSegment expressionSegment = ((ExpressionProjectionSegment) projectionSegment).getExpr();
            literalToken.addValue(((LiteralExpressionSegment) expressionSegment).getLiterals(), alias);
        } else {
            literalToken.addValue(((ParameterMarkerExpressionSegment) projectionSegment).getParameterMarkerType().getMarker(), alias);
        }
    }
    
    private void addCipherColumn(final String schemaName, final String tableName, final EncryptColumn encryptColumn, final EncryptSelectValueToken literalToken,
                                 final ProjectionSegment projectionSegment, final String columnLabel, final DatabaseType databaseType) {
        String alias = EncryptDerivedColumnSuffix.CIPHER.getDerivedColumnName(columnLabel, databaseType);
        if (projectionSegment instanceof ExpressionProjectionSegment) {
            ExpressionSegment expressionSegment = ((ExpressionProjectionSegment) projectionSegment).getExpr();
            literalToken.addValue(encryptColumn.getCipher().encrypt(database.getName(), schemaName, tableName, encryptColumn.getName(), ((LiteralExpressionSegment) expressionSegment).getLiterals()),
                    alias);
        } else {
            literalToken.addValue(((ParameterMarkerExpressionSegment) projectionSegment).getParameterMarkerType().getMarker(), alias);
        }
    }
    
    private void addAssistedQueryColumn(final String schemaName, final String tableName, final String columnName, final EncryptSelectValueToken literalToken,
                                        final ProjectionSegment projectionSegment, final AssistedQueryColumnItem assistedQueryColumnItem, final String columnLabel, final DatabaseType databaseType) {
        String alias = EncryptDerivedColumnSuffix.ASSISTED_QUERY.getDerivedColumnName(columnLabel, databaseType);
        if (projectionSegment instanceof ExpressionProjectionSegment) {
            ExpressionSegment expressionSegment = ((ExpressionProjectionSegment) projectionSegment).getExpr();
            literalToken.addValue(assistedQueryColumnItem.encrypt(database.getName(), schemaName, tableName, columnName, ((LiteralExpressionSegment) expressionSegment).getLiterals()), alias);
        } else {
            literalToken.addValue(((ParameterMarkerExpressionSegment) projectionSegment).getParameterMarkerType().getMarker(), alias);
        }
    }
    
    private void addLikeQueryColumn(final String schemaName, final String tableName, final String columnName, final EncryptSelectValueToken literalToken,
                                    final ProjectionSegment projectionSegment, final LikeQueryColumnItem likeQueryColumnItem, final String columnLabel, final DatabaseType databaseType) {
        String alias = EncryptDerivedColumnSuffix.LIKE_QUERY.getDerivedColumnName(columnLabel, databaseType);
        if (projectionSegment instanceof ExpressionProjectionSegment) {
            ExpressionSegment expressionSegment = ((ExpressionProjectionSegment) projectionSegment).getExpr();
            literalToken.addValue(likeQueryColumnItem.encrypt(database.getName(), schemaName, tableName, columnName, ((LiteralExpressionSegment) expressionSegment).getLiterals()), alias);
        } else {
            literalToken.addValue(((ParameterMarkerExpressionSegment) projectionSegment).getParameterMarkerType().getMarker(), alias);
        }
    }
    
    private void addOrderQueryColumn(final String schemaName, final String tableName, final String columnName, final EncryptSelectValueToken literalToken,
                                     final ProjectionSegment projectionSegment, final OrderQueryColumnItem orderQueryColumnItem, final String columnLabel, final DatabaseType databaseType) {
        String alias = EncryptDerivedColumnSuffix.ORDER_QUERY.getDerivedColumnName(columnLabel, databaseType);
        if (projectionSegment instanceof ExpressionProjectionSegment) {
            ExpressionSegment expressionSegment = ((ExpressionProjectionSegment) projectionSegment).getExpr();
            literalToken.addValue(orderQueryColumnItem.encrypt(database.getName(), schemaName, tableName, columnName, ((LiteralExpressionSegment) expressionSegment).getLiterals()), alias);
        } else {
            literalToken.addValue(((ParameterMarkerExpressionSegment) projectionSegment).getParameterMarkerType().getMarker(), alias);
        }
    }
}
