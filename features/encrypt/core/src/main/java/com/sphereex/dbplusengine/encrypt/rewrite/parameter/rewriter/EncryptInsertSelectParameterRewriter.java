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

package com.sphereex.dbplusengine.encrypt.rewrite.parameter.rewriter;

import lombok.RequiredArgsConstructor;
import org.apache.shardingsphere.encrypt.rule.EncryptRule;
import org.apache.shardingsphere.encrypt.rule.column.EncryptColumn;
import org.apache.shardingsphere.encrypt.rule.table.EncryptTable;
import org.apache.shardingsphere.infra.binder.context.segment.select.projection.Projection;
import org.apache.shardingsphere.infra.binder.context.segment.select.projection.impl.ParameterMarkerProjection;
import org.apache.shardingsphere.infra.binder.context.statement.SQLStatementContext;
import org.apache.shardingsphere.infra.binder.context.statement.dml.InsertStatementContext;
import org.apache.shardingsphere.infra.binder.context.statement.dml.SelectStatementContext;
import org.apache.shardingsphere.infra.database.core.type.DatabaseTypeRegistry;
import org.apache.shardingsphere.infra.rewrite.parameter.builder.ParameterBuilder;
import org.apache.shardingsphere.infra.rewrite.parameter.builder.impl.StandardParameterBuilder;
import org.apache.shardingsphere.infra.rewrite.parameter.rewriter.ParameterRewriter;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;

/**
 * Insert select parameter rewriter for encrypt.
 */
@RequiredArgsConstructor
public final class EncryptInsertSelectParameterRewriter implements ParameterRewriter {
    
    private final EncryptRule rule;
    
    private final String databaseName;
    
    @Override
    public boolean isNeedRewrite(final SQLStatementContext sqlStatementContext) {
        return sqlStatementContext instanceof InsertStatementContext && null != ((InsertStatementContext) sqlStatementContext).getInsertSelectContext()
                && containsParameterMarkerExpression(((InsertStatementContext) sqlStatementContext).getInsertSelectContext().getSelectStatementContext());
    }
    
    private boolean containsParameterMarkerExpression(final SelectStatementContext selectStatementContext) {
        for (Projection each : selectStatementContext.getProjectionsContext().getProjections()) {
            if (each instanceof ParameterMarkerProjection) {
                return true;
            }
        }
        return selectStatementContext.getSubqueryContexts().values().stream().map(this::containsParameterMarkerExpression).findAny().isPresent();
    }
    
    @Override
    public void rewrite(final ParameterBuilder paramBuilder, final SQLStatementContext sqlStatementContext, final List<Object> params) {
        InsertStatementContext insertStatementContext = (InsertStatementContext) sqlStatementContext;
        String tableName = insertStatementContext.getSqlStatement().getTable().map(optional -> optional.getTableName().getIdentifier().getValue())
                .orElseThrow(() -> new IllegalArgumentException("Can not find table name."));
        Optional<EncryptTable> encryptTable = rule.findEncryptTable(tableName);
        if (!encryptTable.isPresent()) {
            return;
        }
        String schemaName = insertStatementContext.getTablesContext().getSchemaName()
                .orElseGet(() -> new DatabaseTypeRegistry(insertStatementContext.getDatabaseType()).getDefaultSchemaName(databaseName));
        SelectStatementContext selectStatementContext = insertStatementContext.getInsertSelectContext().getSelectStatementContext();
        rewrite((StandardParameterBuilder) paramBuilder, selectStatementContext, insertStatementContext, tableName, schemaName);
    }
    
    private void rewrite(final StandardParameterBuilder paramBuilder, final SelectStatementContext selectStatementContext, final InsertStatementContext insertStatementContext, final String tableName,
                         final String schemaName) {
        if (!selectStatementContext.isContainsCombine()) {
            rewriteParameters(paramBuilder, selectStatementContext, insertStatementContext, tableName, schemaName);
        }
        selectStatementContext.getSubqueryContexts().values().forEach(each -> rewrite(paramBuilder, each, insertStatementContext, tableName, schemaName));
    }
    
    private void rewriteParameters(final StandardParameterBuilder paramBuilder, final SelectStatementContext selectStatementContext, final InsertStatementContext insertStatementContext,
                                   final String tableName, final String schemaName) {
        int columnIndex = 0;
        for (Projection each : selectStatementContext.getProjectionsContext().getProjections()) {
            if (!(each instanceof ParameterMarkerProjection)) {
                columnIndex++;
                continue;
            }
            String columnName = insertStatementContext.getColumnNames().get(columnIndex++);
            if (rule.findEncryptTable(tableName).map(optional -> optional.isEncryptColumn(columnName)).orElse(false)) {
                encryptInsertValues(paramBuilder, insertStatementContext, (ParameterMarkerProjection) each, schemaName, tableName, columnName);
            }
        }
    }
    
    private void encryptInsertValues(final StandardParameterBuilder standardParamBuilder, final InsertStatementContext insertStatementContext,
                                     final ParameterMarkerProjection parameterMarkerProjection, final String schemaName, final String tableName, final String columnName) {
        EncryptColumn encryptColumn = rule.getEncryptTable(tableName).getEncryptColumn(columnName);
        Object literalValue = insertStatementContext.getInsertSelectContext().getParameters().get(parameterMarkerProjection.getParameterMarkerIndex());
        encryptInsertValue(encryptColumn, parameterMarkerProjection.getParameterMarkerIndex(), literalValue, standardParamBuilder, schemaName, tableName);
    }
    
    private void encryptInsertValue(final EncryptColumn encryptColumn, final int paramIndex, final Object originalValue,
                                    final StandardParameterBuilder paramBuilder, final String schemaName, final String tableName) {
        String columnName = encryptColumn.getName();
        paramBuilder.addReplacedParameters(paramIndex, encryptColumn.getCipher().encrypt(databaseName, schemaName, tableName, columnName, originalValue));
        Collection<Object> addedParams = new LinkedList<>();
        if (encryptColumn.getAssistedQuery().isPresent()) {
            addedParams.add(encryptColumn.getAssistedQuery().get().encrypt(databaseName, schemaName, tableName, columnName, originalValue));
        }
        if (encryptColumn.getLikeQuery().isPresent()) {
            addedParams.add(encryptColumn.getLikeQuery().get().encrypt(databaseName, schemaName, tableName, columnName, originalValue));
        }
        if (encryptColumn.getOrderQuery().isPresent()) {
            addedParams.add(encryptColumn.getOrderQuery().get().encrypt(databaseName, schemaName, tableName, columnName, originalValue));
        }
        if (encryptColumn.getPlain().isPresent()) {
            addedParams.add(originalValue);
        }
        if (!addedParams.isEmpty()) {
            if (!paramBuilder.getAddedIndexAndParameters().containsKey(paramIndex)) {
                paramBuilder.getAddedIndexAndParameters().put(paramIndex, new LinkedList<>());
            }
            paramBuilder.getAddedIndexAndParameters().get(paramIndex).addAll(addedParams);
        }
    }
}
