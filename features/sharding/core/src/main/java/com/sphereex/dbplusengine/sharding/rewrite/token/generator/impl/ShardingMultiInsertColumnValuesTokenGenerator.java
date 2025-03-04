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

package com.sphereex.dbplusengine.sharding.rewrite.token.generator.impl;

import com.sphereex.dbplusengine.sharding.rewrite.token.pojo.ShardingMultiInsertColumnValue;
import com.sphereex.dbplusengine.sharding.rewrite.token.pojo.ShardingMultiInsertColumnValuesToken;
import lombok.Setter;
import org.apache.shardingsphere.infra.annotation.HighFrequencyInvocation;
import org.apache.shardingsphere.infra.binder.context.segment.insert.keygen.GeneratedKeyContext;
import org.apache.shardingsphere.infra.binder.context.segment.insert.values.expression.DerivedLiteralExpressionSegment;
import org.apache.shardingsphere.infra.binder.context.segment.insert.values.expression.DerivedParameterMarkerExpressionSegment;
import org.apache.shardingsphere.infra.binder.context.segment.insert.values.expression.DerivedSimpleExpressionSegment;
import org.apache.shardingsphere.infra.binder.context.statement.SQLStatementContext;
import org.apache.shardingsphere.infra.binder.context.statement.dml.InsertStatementContext;
import org.apache.shardingsphere.infra.datanode.DataNode;
import org.apache.shardingsphere.infra.rewrite.sql.token.common.generator.OptionalSQLTokenGenerator;
import org.apache.shardingsphere.infra.rewrite.sql.token.common.generator.aware.RouteContextAware;
import org.apache.shardingsphere.infra.rewrite.sql.token.common.pojo.SQLToken;
import org.apache.shardingsphere.infra.rewrite.sql.token.common.pojo.generic.InsertValuesToken;
import org.apache.shardingsphere.infra.route.context.RouteContext;
import org.apache.shardingsphere.sql.parser.statement.core.segment.dml.expr.ExpressionSegment;
import org.apache.shardingsphere.sql.parser.statement.core.segment.dml.table.MultiTableInsertIntoSegment;
import org.apache.shardingsphere.sql.parser.statement.core.segment.generic.table.SimpleTableSegment;
import org.apache.shardingsphere.sql.parser.statement.core.segment.generic.table.TableNameSegment;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * Multi insert values token generator for sharding.
 */
@HighFrequencyInvocation
@Setter
public final class ShardingMultiInsertColumnValuesTokenGenerator implements OptionalSQLTokenGenerator<InsertStatementContext>, RouteContextAware {
    
    private RouteContext routeContext;
    
    @Override
    public boolean isGenerateSQLToken(final SQLStatementContext sqlStatementContext) {
        return sqlStatementContext instanceof InsertStatementContext && !(((InsertStatementContext) sqlStatementContext).getMultiInsertStatementContexts().isEmpty());
    }
    
    @Override
    public SQLToken generateSQLToken(final InsertStatementContext insertStatementContext) {
        MultiTableInsertIntoSegment multiTableInsertIntoSegment =
                insertStatementContext.getSqlStatement().getMultiTableInsertIntoSegment().orElseThrow(() -> new IllegalStateException("Can not get multi table insert into segment."));
        InsertValuesToken result = new ShardingMultiInsertColumnValuesToken(multiTableInsertIntoSegment.getStartIndex(), multiTableInsertIntoSegment.getStopIndex());
        Iterator<Collection<DataNode>> dataNodesIterator = routeContext.getOriginalDataNodes().isEmpty() ? Collections.emptyIterator() : routeContext.getOriginalDataNodes().iterator();
        for (InsertStatementContext each : insertStatementContext.getMultiInsertStatementContexts()) {
            List<ExpressionSegment> expressionSegments = new ArrayList<>(each.getInsertValueContexts().iterator().next().getValueExpressions());
            Collection<DataNode> dataNodes = dataNodesIterator.hasNext() ? dataNodesIterator.next() : Collections.emptyList();
            TableNameSegment tableName = each.getSqlStatement().getTable().map(SimpleTableSegment::getTableName).orElseThrow(() -> new IllegalStateException("Can not get table name."));
            List<String> columnNames = new ArrayList<>(each.getColumnNames());
            appendGeneratedKey(each, columnNames, expressionSegments);
            result.getInsertValues().add(new ShardingMultiInsertColumnValue(expressionSegments, tableName, columnNames, dataNodes));
        }
        return result;
    }
    
    private void appendGeneratedKey(final InsertStatementContext insertStatementContext, final List<String> columnNames, final List<ExpressionSegment> expressionSegments) {
        if (!insertStatementContext.getGeneratedKeyContext().isPresent()) {
            return;
        }
        GeneratedKeyContext generatedKeyContext = insertStatementContext.getGeneratedKeyContext().get();
        columnNames.add(generatedKeyContext.getColumnName());
        DerivedSimpleExpressionSegment expressionSegment = insertStatementContext.getGroupedParameters().iterator().next().isEmpty()
                ? new DerivedLiteralExpressionSegment(generatedKeyContext.getGeneratedValues().iterator().next())
                : new DerivedParameterMarkerExpressionSegment(insertStatementContext.getInsertValueContexts().iterator().next().getParameterCount());
        expressionSegments.add(expressionSegment);
    }
}
