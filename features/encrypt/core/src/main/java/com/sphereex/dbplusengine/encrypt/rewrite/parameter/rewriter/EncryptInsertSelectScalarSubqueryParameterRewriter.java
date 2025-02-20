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
import org.apache.shardingsphere.infra.binder.context.segment.select.projection.impl.ColumnProjection;
import org.apache.shardingsphere.infra.binder.context.segment.select.projection.impl.SubqueryProjection;
import org.apache.shardingsphere.infra.binder.context.statement.SQLStatementContext;
import org.apache.shardingsphere.infra.binder.context.statement.dml.InsertStatementContext;
import org.apache.shardingsphere.infra.binder.context.statement.dml.SelectStatementContext;
import org.apache.shardingsphere.infra.exception.core.ShardingSpherePreconditions;
import org.apache.shardingsphere.infra.rewrite.parameter.builder.ParameterBuilder;
import org.apache.shardingsphere.infra.rewrite.parameter.builder.impl.StandardParameterBuilder;
import org.apache.shardingsphere.infra.rewrite.parameter.rewriter.ParameterRewriter;
import org.apache.shardingsphere.sql.parser.statement.core.segment.dml.item.SubqueryProjectionSegment;
import org.apache.shardingsphere.sql.parser.statement.core.segment.generic.ParameterMarkerSegment;
import org.apache.shardingsphere.sql.parser.statement.core.segment.generic.bound.ColumnSegmentBoundInfo;
import org.apache.shardingsphere.sql.parser.statement.core.value.identifier.IdentifierValue;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Insert select scalar subquery parameter rewriter for encrypt.
 */
@RequiredArgsConstructor
public final class EncryptInsertSelectScalarSubqueryParameterRewriter implements ParameterRewriter {
    
    private final EncryptRule rule;
    
    private final Map<String, EncryptRule> databaseEncryptRules;
    
    @Override
    public boolean isNeedRewrite(final SQLStatementContext sqlStatementContext) {
        return sqlStatementContext instanceof InsertStatementContext && null != ((InsertStatementContext) sqlStatementContext).getInsertSelectContext()
                && ((InsertStatementContext) sqlStatementContext).getSqlStatement().getTable().isPresent()
                && containsScalarSubquery(((InsertStatementContext) sqlStatementContext).getInsertSelectContext().getSelectStatementContext());
    }
    
    private boolean containsScalarSubquery(final SelectStatementContext selectStatementContext) {
        for (Projection each : selectStatementContext.getProjectionsContext().getExpandProjections()) {
            if (each instanceof SubqueryProjection) {
                return true;
            }
        }
        return false;
    }
    
    @Override
    public void rewrite(final ParameterBuilder paramBuilder, final SQLStatementContext sqlStatementContext, final List<Object> params) {
        InsertStatementContext insertStatementContext = (InsertStatementContext) sqlStatementContext;
        ShardingSpherePreconditions.checkNotNull(insertStatementContext.getInsertSelectContext(), () -> new IllegalStateException("Insert select segment is required."));
        SelectStatementContext selectStatementContext = insertStatementContext.getInsertSelectContext().getSelectStatementContext();
        for (Projection each : insertStatementContext.getInsertSelectContext().getSelectStatementContext().getProjectionsContext().getExpandProjections()) {
            if (!(each instanceof SubqueryProjection) || !selectStatementContext.getSubqueryContexts().containsKey(((SubqueryProjection) each).getSubquerySegment().getStartIndex())) {
                continue;
            }
            SubqueryProjectionSegment scalarSubquerySegment = ((SubqueryProjection) each).getSubquerySegment();
            SelectStatementContext scalarSubqueryContext = selectStatementContext.getSubqueryContexts().get(scalarSubquerySegment.getStartIndex());
            ColumnSegmentBoundInfo columnBoundedInfo = getColumnSegmentBoundInfo(scalarSubqueryContext.getProjectionsContext().getExpandProjections().iterator().next());
            EncryptRule encryptRule = Optional.ofNullable(columnBoundedInfo.getOriginalDatabase()).map(IdentifierValue::getValue).map(databaseEncryptRules::get).orElse(this.rule);
            Optional<EncryptTable> encryptTable = encryptRule.findEncryptTable(columnBoundedInfo.getOriginalTable().getValue());
            if (!encryptTable.isPresent() || !encryptTable.get().isEncryptColumn(columnBoundedInfo.getOriginalColumn().getValue())) {
                continue;
            }
            EncryptColumn encryptColumn = encryptTable.get().getEncryptColumn(columnBoundedInfo.getOriginalColumn().getValue());
            List<Object> parameters = paramBuilder.getParameters();
            List<Integer> sortedParameterIndexes = getSortedParameterIndexes(selectStatementContext, scalarSubquerySegment);
            appendRewrittenParameters(encryptColumn, (StandardParameterBuilder) paramBuilder, sortedParameterIndexes, parameters);
        }
    }
    
    private void appendRewrittenParameters(final EncryptColumn encryptColumn, final StandardParameterBuilder paramBuilder, final List<Integer> sortedParameterIndexes, final List<Object> parameters) {
        if (encryptColumn.getPlain().isPresent()) {
            rewriteParameters(paramBuilder, sortedParameterIndexes, parameters);
        }
        if (encryptColumn.getAssistedQuery().isPresent()) {
            rewriteParameters(paramBuilder, sortedParameterIndexes, parameters);
        }
        if (encryptColumn.getOrderQuery().isPresent()) {
            rewriteParameters(paramBuilder, sortedParameterIndexes, parameters);
        }
        if (encryptColumn.getLikeQuery().isPresent()) {
            rewriteParameters(paramBuilder, sortedParameterIndexes, parameters);
        }
    }
    
    private ColumnSegmentBoundInfo getColumnSegmentBoundInfo(final Projection projection) {
        if (projection instanceof ColumnProjection) {
            return ((ColumnProjection) projection).getColumnBoundInfo();
        }
        if (projection instanceof SubqueryProjection) {
            return getColumnSegmentBoundInfo(((SubqueryProjection) projection).getProjection());
        }
        return new ColumnSegmentBoundInfo(new IdentifierValue(projection.getColumnLabel()));
    }
    
    private void rewriteParameters(final StandardParameterBuilder paramBuilder, final List<Integer> sortedParameterIndexes, final List<Object> parameters) {
        Collection<Object> copiedParameters = new LinkedList<>();
        for (Integer each : sortedParameterIndexes) {
            copiedParameters.add(parameters.get(each));
        }
        paramBuilder.addAddedParameters(sortedParameterIndexes.get(sortedParameterIndexes.size() - 1), copiedParameters);
    }
    
    private List<Integer> getSortedParameterIndexes(final SelectStatementContext selectStatementContext, final SubqueryProjectionSegment subqueryProjection) {
        List<Integer> result = new LinkedList<>();
        for (ParameterMarkerSegment each : selectStatementContext.getSqlStatement().getParameterMarkerSegments()) {
            if (subqueryProjection.getStartIndex() <= each.getStartIndex() && subqueryProjection.getStopIndex() >= each.getStopIndex()) {
                result.add(each.getParameterIndex());
            }
        }
        Collections.sort(result);
        return result;
    }
}
