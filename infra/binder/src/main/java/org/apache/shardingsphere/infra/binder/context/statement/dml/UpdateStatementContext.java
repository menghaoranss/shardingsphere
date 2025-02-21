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

package org.apache.shardingsphere.infra.binder.context.statement.dml;

import com.sphereex.dbplusengine.SphereEx;
import lombok.Getter;
import org.apache.shardingsphere.infra.binder.context.segment.table.TablesContext;
import org.apache.shardingsphere.infra.binder.context.statement.CommonSQLStatementContext;
import org.apache.shardingsphere.infra.binder.context.type.TableAvailable;
import org.apache.shardingsphere.infra.binder.context.type.WhereAvailable;
import org.apache.shardingsphere.infra.binder.context.type.WithAvailable;
import org.apache.shardingsphere.sql.parser.statement.core.extractor.ColumnExtractor;
import org.apache.shardingsphere.sql.parser.statement.core.extractor.ExpressionExtractor;
import org.apache.shardingsphere.sql.parser.statement.core.extractor.TableExtractor;
import org.apache.shardingsphere.sql.parser.statement.core.segment.dml.assignment.ColumnAssignmentSegment;
import org.apache.shardingsphere.sql.parser.statement.core.segment.dml.assignment.SetAssignmentSegment;
import org.apache.shardingsphere.sql.parser.statement.core.segment.dml.column.ColumnSegment;
import org.apache.shardingsphere.sql.parser.statement.core.segment.dml.expr.BinaryOperationExpression;
import org.apache.shardingsphere.sql.parser.statement.core.segment.dml.expr.ExpressionSegment;
import org.apache.shardingsphere.sql.parser.statement.core.segment.dml.expr.FunctionSegment;
import org.apache.shardingsphere.sql.parser.statement.core.segment.dml.expr.simple.LiteralExpressionSegment;
import org.apache.shardingsphere.sql.parser.statement.core.segment.dml.expr.simple.ParameterMarkerExpressionSegment;
import org.apache.shardingsphere.sql.parser.statement.core.segment.dml.predicate.WhereSegment;
import org.apache.shardingsphere.sql.parser.statement.core.segment.generic.WithSegment;
import org.apache.shardingsphere.sql.parser.statement.core.segment.generic.table.SimpleTableSegment;
import org.apache.shardingsphere.sql.parser.statement.core.statement.dml.UpdateStatement;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;

/**
 * Update SQL statement context.
 */
@Getter
public final class UpdateStatementContext extends CommonSQLStatementContext implements TableAvailable, WhereAvailable, WithAvailable {
    
    private final TablesContext tablesContext;
    
    private final Collection<WhereSegment> whereSegments = new LinkedList<>();
    
    private final Collection<ColumnSegment> columnSegments = new LinkedList<>();
    
    private final Collection<BinaryOperationExpression> joinConditions = new LinkedList<>();
    
    @SphereEx
    private final Collection<ColumnSegment> columnSegmentsForUDF = new LinkedList<>();
    
    @SphereEx
    private final List<ParameterMarkerExpressionSegment> parameterMarkerExpressions = new LinkedList<>();
    
    public UpdateStatementContext(final UpdateStatement sqlStatement) {
        super(sqlStatement);
        tablesContext = new TablesContext(getAllSimpleTableSegments());
        getSqlStatement().getWhere().ifPresent(whereSegments::add);
        ColumnExtractor.extractColumnSegments(columnSegments, whereSegments);
        ExpressionExtractor.extractJoinConditions(joinConditions, whereSegments);
        // SPEX ADDED: BEGIN
        sqlStatement.getWhere().ifPresent(optional -> ColumnExtractor.extractFromWhere(columnSegmentsForUDF, optional, true));
        parameterMarkerExpressions.addAll(ExpressionExtractor.getParameterMarkerExpressions(getAllSetAssignmentExpressions(sqlStatement)));
        // SPEX ADDED: END
    }
    
    private Collection<SimpleTableSegment> getAllSimpleTableSegments() {
        TableExtractor tableExtractor = new TableExtractor();
        tableExtractor.extractTablesFromUpdate(getSqlStatement());
        return tableExtractor.getRewriteTables();
    }
    
    @Override
    public UpdateStatement getSqlStatement() {
        return (UpdateStatement) super.getSqlStatement();
    }
    
    @Override
    public Collection<WhereSegment> getWhereSegments() {
        return whereSegments;
    }
    
    @Override
    public Collection<ColumnSegment> getColumnSegments() {
        return columnSegments;
    }
    
    @Override
    public Optional<WithSegment> getWith() {
        return getSqlStatement().getWithSegment();
    }
    
    /**
     * Get literal value.
     *
     * @param columnName column name
     * @param params parameters
     * @return literal value
     */
    @SphereEx
    public Optional<Object> getLiteralValue(final String columnName, final List<Object> params) {
        for (ColumnAssignmentSegment each : getSqlStatement().getSetAssignment().getAssignments()) {
            if (!columnName.equalsIgnoreCase(each.getColumns().get(0).getIdentifier().getValue())) {
                continue;
            }
            ExpressionSegment expression = each.getValue();
            if (expression instanceof FunctionSegment || expression instanceof ParameterMarkerExpressionSegment) {
                return Optional.ofNullable(params.get(getParameterMarkerIndex(expression)));
            }
            if (expression instanceof LiteralExpressionSegment) {
                return Optional.ofNullable(((LiteralExpressionSegment) expression).getLiterals());
            }
        }
        return Optional.empty();
    }
    
    @SphereEx
    private int getParameterMarkerIndex(final ExpressionSegment expressionSegment) {
        if (expressionSegment instanceof ParameterMarkerExpressionSegment) {
            return parameterMarkerExpressions.indexOf((ParameterMarkerExpressionSegment) expressionSegment);
        }
        for (ExpressionSegment each : ((FunctionSegment) expressionSegment).getParameters()) {
            if (each instanceof ParameterMarkerExpressionSegment) {
                return parameterMarkerExpressions.indexOf((ParameterMarkerExpressionSegment) each);
            }
        }
        return 0;
    }
    
    @SphereEx
    private List<ExpressionSegment> getAllSetAssignmentExpressions(final UpdateStatement sqlStatement) {
        SetAssignmentSegment setAssignment = sqlStatement.getSetAssignment();
        List<ExpressionSegment> result = new ArrayList<>(setAssignment.getAssignments().size());
        for (ColumnAssignmentSegment each : setAssignment.getAssignments()) {
            result.add(each.getValue());
        }
        return result;
    }
}
