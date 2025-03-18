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

import com.sphereex.dbplusengine.encrypt.rule.column.item.PlainColumnItem;
import com.sphereex.dbplusengine.infra.binder.context.type.FunctionAvailable;
import lombok.RequiredArgsConstructor;
import org.apache.shardingsphere.encrypt.exception.metadata.MissingMatchedEncryptQueryAlgorithmException;
import org.apache.shardingsphere.encrypt.rule.EncryptRule;
import org.apache.shardingsphere.encrypt.rule.column.EncryptColumn;
import org.apache.shardingsphere.encrypt.rule.table.EncryptTable;
import org.apache.shardingsphere.infra.binder.context.extractor.SQLStatementContextExtractor;
import org.apache.shardingsphere.infra.binder.context.statement.SQLStatementContext;
import org.apache.shardingsphere.infra.binder.context.statement.dml.SelectStatementContext;
import org.apache.shardingsphere.infra.exception.core.ShardingSpherePreconditions;
import org.apache.shardingsphere.infra.rewrite.parameter.builder.ParameterBuilder;
import org.apache.shardingsphere.infra.rewrite.parameter.builder.impl.StandardParameterBuilder;
import org.apache.shardingsphere.infra.rewrite.parameter.rewriter.ParameterRewriter;
import org.apache.shardingsphere.sql.parser.statement.core.segment.dml.column.ColumnSegment;
import org.apache.shardingsphere.sql.parser.statement.core.segment.dml.expr.ExpressionSegment;
import org.apache.shardingsphere.sql.parser.statement.core.segment.dml.expr.FunctionSegment;
import org.apache.shardingsphere.sql.parser.statement.core.segment.dml.expr.simple.ParameterMarkerExpressionSegment;
import org.apache.shardingsphere.sql.parser.statement.core.segment.generic.bound.ColumnSegmentBoundInfo;

import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Function parameter rewriter for encrypt.
 */
@RequiredArgsConstructor
public final class EncryptFunctionParameterRewriter implements ParameterRewriter {
    
    private final EncryptRule rule;
    
    private final String databaseName;
    
    private final Map<String, EncryptRule> databaseEncryptRules;
    
    @Override
    public boolean isNeedRewrite(final SQLStatementContext sqlStatementContext) {
        return sqlStatementContext instanceof FunctionAvailable;
    }
    
    @Override
    public void rewrite(final ParameterBuilder paramBuilder, final SQLStatementContext sqlStatementContext, final List<Object> params) {
        Collection<ExpressionSegment> functionSegments = getAllFunctionSegments(sqlStatementContext);
        for (ExpressionSegment each : functionSegments) {
            if (each instanceof FunctionSegment) {
                rewrite((StandardParameterBuilder) paramBuilder, (FunctionSegment) each, params);
            }
        }
    }
    
    private void rewrite(final StandardParameterBuilder paramBuilder, final FunctionSegment functionSegment, final List<Object> params) {
        // TODO support more functions parameter rewrite
        if (!"STRCMP".equalsIgnoreCase(functionSegment.getFunctionName())) {
            return;
        }
        Iterator<ExpressionSegment> paramIterator = functionSegment.getParameters().iterator();
        ExpressionSegment param1 = paramIterator.next();
        ExpressionSegment param2 = paramIterator.next();
        if (!(param1 instanceof ColumnSegment) && !(param2 instanceof ColumnSegment)) {
            return;
        }
        if (!(param1 instanceof ParameterMarkerExpressionSegment) && !(param2 instanceof ParameterMarkerExpressionSegment)) {
            return;
        }
        ColumnSegment columnSegment = param1 instanceof ColumnSegment ? (ColumnSegment) param1 : (ColumnSegment) param2;
        String columnName = columnSegment.getColumnBoundInfo().getOriginalColumn().getValue();
        String tableName = columnSegment.getColumnBoundInfo().getOriginalTable().getValue();
        Optional<EncryptTable> encryptTable = databaseEncryptRules.getOrDefault(columnSegment.getColumnBoundInfo().getOriginalDatabase().getValue(), rule).findEncryptTable(tableName);
        if (!encryptTable.isPresent() || !encryptTable.get().isEncryptColumn(columnName)) {
            return;
        }
        EncryptColumn encryptColumn = encryptTable.get().getEncryptColumn(columnName);
        Optional<PlainColumnItem> plainColumnItem = encryptColumn.getPlain();
        if (plainColumnItem.isPresent() && plainColumnItem.get().isQueryWithPlain()) {
            return;
        }
        ShardingSpherePreconditions.checkState(encryptColumn.getOrderQuery().isPresent() || encryptColumn.getCipher().getEncryptor().getMetaData().isSupportOrder(),
                () -> new MissingMatchedEncryptQueryAlgorithmException(tableName, columnSegment.getIdentifier().getValue(), "ORDER"));
        ParameterMarkerExpressionSegment parameterMarker = columnSegment.equals(param1) ? (ParameterMarkerExpressionSegment) param2 : (ParameterMarkerExpressionSegment) param1;
        paramBuilder.addReplacedParameters(parameterMarker.getParameterIndex(),
                getEncryptValue(encryptColumn, String.valueOf(params.get(parameterMarker.getParameterIndex())), databaseName, columnSegment));
    }
    
    private Object getEncryptValue(final EncryptColumn encryptColumn, final Object originalValue, final String databaseName, final ColumnSegment columnSegment) {
        ColumnSegmentBoundInfo columnBoundInfo = columnSegment.getColumnBoundInfo();
        String schemaName = columnBoundInfo.getOriginalSchema().getValue();
        String tableName = columnBoundInfo.getOriginalTable().getValue();
        String columnName = columnBoundInfo.getOriginalColumn().getValue();
        if (encryptColumn.getOrderQuery().isPresent()) {
            return encryptColumn.getOrderQuery().get().encrypt(databaseName, schemaName, tableName, columnName, originalValue);
        }
        return encryptColumn.getCipher().encrypt(databaseName, schemaName, tableName, columnName, originalValue);
    }
    
    private Collection<ExpressionSegment> getAllFunctionSegments(final SQLStatementContext sqlStatementContext) {
        Collection<ExpressionSegment> result = new LinkedList<>(((FunctionAvailable) sqlStatementContext).getFunctionSegments());
        Collection<SelectStatementContext> allSubqueryContexts = SQLStatementContextExtractor.getAllSubqueryContexts(sqlStatementContext);
        for (SelectStatementContext each : allSubqueryContexts) {
            result.addAll(each.getFunctionSegments());
        }
        return result;
    }
}
