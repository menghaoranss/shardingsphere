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

package com.sphereex.dbplusengine.encrypt.rewrite.token.cryptographic.generator.insert;

import com.sphereex.dbplusengine.encrypt.rewrite.token.cryptographic.pojo.EncryptInsertSelectValueToken;
import com.sphereex.dbplusengine.encrypt.rule.column.item.OrderQueryColumnItem;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import org.apache.shardingsphere.encrypt.rule.EncryptRule;
import org.apache.shardingsphere.encrypt.rule.column.EncryptColumn;
import org.apache.shardingsphere.encrypt.rule.column.item.AssistedQueryColumnItem;
import org.apache.shardingsphere.encrypt.rule.column.item.LikeQueryColumnItem;
import org.apache.shardingsphere.encrypt.rule.table.EncryptTable;
import org.apache.shardingsphere.infra.binder.context.segment.insert.values.InsertSelectContext;
import org.apache.shardingsphere.infra.binder.context.segment.select.projection.Projection;
import org.apache.shardingsphere.infra.binder.context.segment.select.projection.impl.ExpressionProjection;
import org.apache.shardingsphere.infra.binder.context.segment.select.projection.impl.ParameterMarkerProjection;
import org.apache.shardingsphere.infra.binder.context.statement.SQLStatementContext;
import org.apache.shardingsphere.infra.binder.context.statement.dml.InsertStatementContext;
import org.apache.shardingsphere.infra.binder.context.statement.dml.SelectStatementContext;
import org.apache.shardingsphere.infra.database.core.type.DatabaseTypeRegistry;
import org.apache.shardingsphere.infra.exception.core.ShardingSpherePreconditions;
import org.apache.shardingsphere.infra.metadata.database.ShardingSphereDatabase;
import org.apache.shardingsphere.infra.rewrite.sql.token.common.generator.CollectionSQLTokenGenerator;
import org.apache.shardingsphere.infra.rewrite.sql.token.common.pojo.SQLToken;
import org.apache.shardingsphere.sql.parser.statement.core.segment.dml.combine.CombineSegment;
import org.apache.shardingsphere.sql.parser.statement.core.segment.dml.expr.ExpressionSegment;
import org.apache.shardingsphere.sql.parser.statement.core.segment.dml.expr.simple.LiteralExpressionSegment;
import org.apache.shardingsphere.sql.parser.statement.core.segment.dml.expr.simple.ParameterMarkerExpressionSegment;
import org.apache.shardingsphere.sql.parser.statement.core.segment.dml.item.ExpressionProjectionSegment;
import org.apache.shardingsphere.sql.parser.statement.core.segment.dml.item.ProjectionSegment;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;

/**
 * Insert select value token generator for encrypt.
 */
@RequiredArgsConstructor
@Setter
public final class EncryptInsertSelectValueTokenGenerator implements CollectionSQLTokenGenerator<InsertStatementContext> {
    
    private final EncryptRule rule;
    
    private final ShardingSphereDatabase database;
    
    @Override
    public boolean isGenerateSQLToken(final SQLStatementContext sqlStatementContext) {
        return sqlStatementContext instanceof InsertStatementContext && null != ((InsertStatementContext) sqlStatementContext).getInsertSelectContext()
                && ((InsertStatementContext) sqlStatementContext).getSqlStatement().getTable().isPresent()
                && containsLiteralOrParameterMarker(((InsertStatementContext) sqlStatementContext).getInsertSelectContext());
    }
    
    private boolean containsLiteralOrParameterMarker(final InsertSelectContext insertSelectContext) {
        for (Projection each : insertSelectContext.getSelectStatementContext().getProjectionsContext().getProjections()) {
            if (isLiteralOrParameterMarkerExpression(each)) {
                return true;
            }
        }
        return false;
    }
    
    private boolean isLiteralOrParameterMarkerExpression(final Projection projection) {
        if (projection instanceof ExpressionProjection) {
            return ((ExpressionProjection) projection).getExpressionSegment().getExpr() instanceof LiteralExpressionSegment;
        }
        return projection instanceof ParameterMarkerProjection;
    }
    
    @Override
    public Collection<SQLToken> generateSQLTokens(final InsertStatementContext insertStatementContext) {
        String tableName = insertStatementContext.getSqlStatement().getTable().map(optional -> optional.getTableName().getIdentifier().getValue())
                .orElseThrow(() -> new IllegalStateException("Can not find table name."));
        Optional<EncryptTable> encryptTable = rule.findEncryptTable(tableName);
        if (!encryptTable.isPresent()) {
            return Collections.emptyList();
        }
        String schemaName = insertStatementContext.getTablesContext().getSchemaName()
                .orElseGet(() -> new DatabaseTypeRegistry(insertStatementContext.getDatabaseType()).getDefaultSchemaName(database.getName()));
        Collection<SQLToken> result = new LinkedList<>();
        ShardingSpherePreconditions.checkNotNull(insertStatementContext.getInsertSelectContext(), () -> new IllegalStateException("Insert select segment is required."));
        SelectStatementContext selectStatementContext = insertStatementContext.getInsertSelectContext().getSelectStatementContext();
        appendSQLTokens(insertStatementContext.getColumnNames(), selectStatementContext, encryptTable.get(), schemaName, result);
        return result;
    }
    
    private void appendSQLTokens(final List<String> insertColumnNames, final SelectStatementContext selectStatementContext, final EncryptTable encryptTable, final String schemaName,
                                 final Collection<SQLToken> sqlTokens) {
        List<ProjectionSegment> projectionSegments = new ArrayList<>(selectStatementContext.getSqlStatement().getProjections().getProjections());
        int columnIndex = 0;
        for (Projection each : selectStatementContext.getProjectionsContext().getExpandProjections()) {
            String columnName = insertColumnNames.get(columnIndex);
            if (!isLiteralOrParameterMarkerExpression(each) || !encryptTable.isEncryptColumn(columnName)) {
                columnIndex++;
                continue;
            }
            ProjectionSegment projectionSegment = projectionSegments.get(columnIndex);
            EncryptInsertSelectValueToken literalToken = new EncryptInsertSelectValueToken(projectionSegment.getStartIndex(), projectionSegment.getStopIndex());
            encryptToken(literalToken, schemaName, encryptTable, insertColumnNames, projectionSegment, columnIndex++);
            sqlTokens.removeIf(sqlToken -> sqlToken.getStartIndex() == literalToken.getStartIndex());
            sqlTokens.add(literalToken);
        }
        if (selectStatementContext.getSqlStatement().getCombine().isPresent()) {
            CombineSegment combineSegment = selectStatementContext.getSqlStatement().getCombine().get();
            Optional.ofNullable(selectStatementContext.getSubqueryContexts().get(combineSegment.getLeft().getStartIndex()))
                    .ifPresent(optional -> appendSQLTokens(insertColumnNames, optional, encryptTable, schemaName, sqlTokens));
            Optional.ofNullable(selectStatementContext.getSubqueryContexts().get(combineSegment.getRight().getStartIndex()))
                    .ifPresent(optional -> appendSQLTokens(insertColumnNames, optional, encryptTable, schemaName, sqlTokens));
        }
    }
    
    private void encryptToken(final EncryptInsertSelectValueToken literalToken, final String schemaName, final EncryptTable encryptTable,
                              final List<String> insertColumnNames, final ProjectionSegment projectionSegment, final int columnIndex) {
        String tableName = encryptTable.getTable();
        String columnName = insertColumnNames.get(columnIndex);
        if (!encryptTable.isEncryptColumn(columnName)) {
            return;
        }
        EncryptColumn encryptColumn = rule.getEncryptTable(tableName).getEncryptColumn(columnName);
        addCipherColumn(schemaName, tableName, encryptColumn, literalToken, projectionSegment);
        encryptColumn.getAssistedQuery().ifPresent(optional -> addAssistedQueryColumn(schemaName, tableName, encryptColumn, literalToken, projectionSegment));
        encryptColumn.getLikeQuery().ifPresent(optional -> addLikeQueryColumn(schemaName, tableName, encryptColumn, literalToken, projectionSegment));
        encryptColumn.getOrderQuery().ifPresent(optional -> addOrderQueryColumn(schemaName, tableName, encryptColumn, literalToken, projectionSegment));
        encryptColumn.getPlain().ifPresent(optional -> addPlainColumn(literalToken, projectionSegment));
    }
    
    private void addCipherColumn(final String schemaName, final String tableName, final EncryptColumn encryptColumn, final EncryptInsertSelectValueToken literalToken,
                                 final ProjectionSegment projectionSegment) {
        if (projectionSegment instanceof ExpressionProjectionSegment) {
            ExpressionSegment expressionSegment = ((ExpressionProjectionSegment) projectionSegment).getExpr();
            literalToken.addValue(encryptColumn.getCipher().encrypt(database.getName(), schemaName, tableName, encryptColumn.getName(), ((LiteralExpressionSegment) expressionSegment).getLiterals()));
        } else {
            literalToken.addValue(((ParameterMarkerExpressionSegment) projectionSegment).getParameterMarkerType().getMarker());
        }
    }
    
    private void addAssistedQueryColumn(final String schemaName, final String tableName, final EncryptColumn encryptColumn, final EncryptInsertSelectValueToken literalToken,
                                        final ProjectionSegment projectionSegment) {
        if (projectionSegment instanceof ExpressionProjectionSegment) {
            ExpressionSegment expressionSegment = ((ExpressionProjectionSegment) projectionSegment).getExpr();
            Optional<AssistedQueryColumnItem> assistedQueryColumnItem = encryptColumn.getAssistedQuery();
            ShardingSpherePreconditions.checkState(assistedQueryColumnItem.isPresent(), () -> new IllegalStateException("Can not find assisted query item config."));
            literalToken
                    .addValue(assistedQueryColumnItem.get().encrypt(database.getName(), schemaName, tableName, encryptColumn.getName(), ((LiteralExpressionSegment) expressionSegment).getLiterals()));
        } else {
            literalToken.addValue(((ParameterMarkerExpressionSegment) projectionSegment).getParameterMarkerType().getMarker());
        }
    }
    
    private void addLikeQueryColumn(final String schemaName, final String tableName, final EncryptColumn encryptColumn, final EncryptInsertSelectValueToken literalToken,
                                    final ProjectionSegment projectionSegment) {
        if (projectionSegment instanceof ExpressionProjectionSegment) {
            ExpressionSegment expressionSegment = ((ExpressionProjectionSegment) projectionSegment).getExpr();
            Optional<LikeQueryColumnItem> likeQueryColumnItem = encryptColumn.getLikeQuery();
            ShardingSpherePreconditions.checkState(likeQueryColumnItem.isPresent(), () -> new IllegalStateException("Can not find like query item config."));
            literalToken.addValue(likeQueryColumnItem.get().encrypt(database.getName(), schemaName, tableName, encryptColumn.getName(), ((LiteralExpressionSegment) expressionSegment).getLiterals()));
        } else {
            literalToken.addValue(((ParameterMarkerExpressionSegment) projectionSegment).getParameterMarkerType().getMarker());
        }
    }
    
    private void addOrderQueryColumn(final String schemaName, final String tableName, final EncryptColumn encryptColumn, final EncryptInsertSelectValueToken literalToken,
                                     final ProjectionSegment projectionSegment) {
        if (projectionSegment instanceof ExpressionProjectionSegment) {
            ExpressionSegment expressionSegment = ((ExpressionProjectionSegment) projectionSegment).getExpr();
            Optional<OrderQueryColumnItem> orderQueryColumnItem = encryptColumn.getOrderQuery();
            ShardingSpherePreconditions.checkState(orderQueryColumnItem.isPresent(), () -> new IllegalStateException("Can not find order query item config."));
            literalToken.addValue(orderQueryColumnItem.get().encrypt(database.getName(), schemaName, tableName, encryptColumn.getName(), ((LiteralExpressionSegment) expressionSegment).getLiterals()));
        } else {
            literalToken.addValue(((ParameterMarkerExpressionSegment) projectionSegment).getParameterMarkerType().getMarker());
        }
    }
    
    private void addPlainColumn(final EncryptInsertSelectValueToken literalToken, final ProjectionSegment projectionSegment) {
        if (projectionSegment instanceof ExpressionProjectionSegment) {
            ExpressionSegment expressionSegment = ((ExpressionProjectionSegment) projectionSegment).getExpr();
            literalToken.addValue(((LiteralExpressionSegment) expressionSegment).getLiterals());
        } else {
            literalToken.addValue(((ParameterMarkerExpressionSegment) projectionSegment).getParameterMarkerType().getMarker());
        }
    }
}
