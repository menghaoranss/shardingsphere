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

package com.sphereex.dbplusengine.infra.binder.engine.statement.dml;

import com.cedarsoftware.util.CaseInsensitiveMap.CaseInsensitiveString;
import com.google.common.collect.LinkedHashMultimap;
import com.google.common.collect.Multimap;
import com.sphereex.dbplusengine.infra.binder.engine.segment.parameter.ParameterMarkerSegmentBinder;
import lombok.SneakyThrows;
import org.apache.shardingsphere.infra.binder.engine.segment.SegmentType;
import org.apache.shardingsphere.infra.binder.engine.segment.dml.column.InsertColumnsSegmentBinder;
import org.apache.shardingsphere.infra.binder.engine.segment.dml.expression.ExpressionSegmentBinder;
import org.apache.shardingsphere.infra.binder.engine.segment.dml.expression.type.ColumnSegmentBinder;
import org.apache.shardingsphere.infra.binder.engine.segment.dml.from.TableSegmentBinder;
import org.apache.shardingsphere.infra.binder.engine.segment.dml.from.context.TableSegmentBinderContext;
import org.apache.shardingsphere.infra.binder.engine.segment.dml.predicate.WhereSegmentBinder;
import org.apache.shardingsphere.infra.binder.engine.statement.SQLStatementBinder;
import org.apache.shardingsphere.infra.binder.engine.statement.SQLStatementBinderContext;
import org.apache.shardingsphere.sql.parser.statement.core.segment.dml.assignment.ColumnAssignmentSegment;
import org.apache.shardingsphere.sql.parser.statement.core.segment.dml.assignment.InsertValuesSegment;
import org.apache.shardingsphere.sql.parser.statement.core.segment.dml.assignment.SetAssignmentSegment;
import org.apache.shardingsphere.sql.parser.statement.core.segment.dml.column.ColumnSegment;
import org.apache.shardingsphere.sql.parser.statement.core.segment.dml.column.InsertColumnsSegment;
import org.apache.shardingsphere.sql.parser.statement.core.segment.dml.expr.ExpressionSegment;
import org.apache.shardingsphere.sql.parser.statement.core.segment.dml.expr.ExpressionWithParamsSegment;
import org.apache.shardingsphere.sql.parser.statement.core.segment.dml.expr.simple.ParameterMarkerExpressionSegment;
import org.apache.shardingsphere.sql.parser.statement.core.segment.dml.item.ColumnProjectionSegment;
import org.apache.shardingsphere.sql.parser.statement.core.segment.dml.item.ProjectionSegment;
import org.apache.shardingsphere.sql.parser.statement.core.segment.generic.ParameterMarkerSegment;
import org.apache.shardingsphere.sql.parser.statement.core.segment.generic.bound.ColumnSegmentBoundInfo;
import org.apache.shardingsphere.sql.parser.statement.core.segment.generic.table.SimpleTableSegment;
import org.apache.shardingsphere.sql.parser.statement.core.segment.generic.table.SubqueryTableSegment;
import org.apache.shardingsphere.sql.parser.statement.core.segment.generic.table.TableSegment;
import org.apache.shardingsphere.sql.parser.statement.core.statement.dml.InsertStatement;
import org.apache.shardingsphere.sql.parser.statement.core.statement.dml.MergeStatement;
import org.apache.shardingsphere.sql.parser.statement.core.statement.dml.UpdateStatement;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Merge statement binder.
 */
public final class MergeStatementBinder implements SQLStatementBinder<MergeStatement> {
    
    @Override
    public MergeStatement bind(final MergeStatement sqlStatement, final SQLStatementBinderContext binderContext) {
        MergeStatement result = copy(sqlStatement);
        Multimap<CaseInsensitiveString, TableSegmentBinderContext> targetTableBinderContexts = LinkedHashMultimap.create();
        TableSegment boundTargetTableSegment = TableSegmentBinder.bind(sqlStatement.getTarget(), binderContext, targetTableBinderContexts, LinkedHashMultimap.create());
        Multimap<CaseInsensitiveString, TableSegmentBinderContext> sourceTableBinderContexts = LinkedHashMultimap.create();
        TableSegment boundSourceTableSegment = TableSegmentBinder.bind(sqlStatement.getSource(), binderContext, sourceTableBinderContexts, LinkedHashMultimap.create());
        result.setTarget(boundTargetTableSegment);
        result.setSource(boundSourceTableSegment);
        Multimap<CaseInsensitiveString, TableSegmentBinderContext> tableBinderContexts = LinkedHashMultimap.create();
        tableBinderContexts.putAll(sourceTableBinderContexts);
        tableBinderContexts.putAll(targetTableBinderContexts);
        if (null != sqlStatement.getExpression()) {
            ExpressionWithParamsSegment expression = new ExpressionWithParamsSegment(sqlStatement.getExpression().getStartIndex(), sqlStatement.getExpression().getStopIndex(),
                    ExpressionSegmentBinder.bind(sqlStatement.getExpression().getExpr(), SegmentType.JOIN_ON, binderContext, tableBinderContexts, LinkedHashMultimap.create()));
            expression.getParameterMarkerSegments().addAll(sqlStatement.getExpression().getParameterMarkerSegments());
            result.setExpression(expression);
        }
        sqlStatement.getInsert().ifPresent(
                optional -> result.setInsert(bindMergeInsert(optional, (SimpleTableSegment) boundTargetTableSegment, binderContext, targetTableBinderContexts, sourceTableBinderContexts)));
        sqlStatement.getUpdate().ifPresent(
                optional -> result.setUpdate(bindMergeUpdate(optional, (SimpleTableSegment) boundTargetTableSegment, binderContext, targetTableBinderContexts, sourceTableBinderContexts)));
        addParameterMarkerSegments(result);
        return result;
    }
    
    @SneakyThrows(ReflectiveOperationException.class)
    private MergeStatement copy(final MergeStatement sqlStatement) {
        MergeStatement result = sqlStatement.getClass().getDeclaredConstructor().newInstance();
        result.getCommentSegments().addAll(sqlStatement.getCommentSegments());
        return result;
    }
    
    private void addParameterMarkerSegments(final MergeStatement mergeStatement) {
        // TODO bind parameter marker segments for merge statement
        mergeStatement.addParameterMarkerSegments(getSourceSubqueryTableProjectionParameterMarkers(mergeStatement.getSource()));
        if (null != mergeStatement.getExpression()) {
            mergeStatement.addParameterMarkerSegments(mergeStatement.getExpression().getParameterMarkerSegments());
        }
        mergeStatement.getInsert().ifPresent(optional -> mergeStatement.addParameterMarkerSegments(optional.getParameterMarkerSegments()));
        mergeStatement.getUpdate().ifPresent(optional -> mergeStatement.addParameterMarkerSegments(optional.getParameterMarkerSegments()));
    }
    
    private Collection<ParameterMarkerSegment> getSourceSubqueryTableProjectionParameterMarkers(final TableSegment tableSegment) {
        if (!(tableSegment instanceof SubqueryTableSegment)) {
            return Collections.emptyList();
        }
        SubqueryTableSegment subqueryTable = (SubqueryTableSegment) tableSegment;
        Collection<ParameterMarkerSegment> result = new LinkedList<>();
        for (ProjectionSegment each : subqueryTable.getSubquery().getSelect().getProjections().getProjections()) {
            if (each instanceof ParameterMarkerExpressionSegment) {
                result.add((ParameterMarkerSegment) each);
            }
        }
        return result;
    }
    
    @SneakyThrows
    private InsertStatement bindMergeInsert(final InsertStatement sqlStatement, final SimpleTableSegment tableSegment, final SQLStatementBinderContext binderContext,
                                            final Multimap<CaseInsensitiveString, TableSegmentBinderContext> targetTableBinderContexts,
                                            final Multimap<CaseInsensitiveString, TableSegmentBinderContext> sourceTableBinderContexts) {
        SQLStatementBinderContext insertStatementBinderContext =
                new SQLStatementBinderContext(binderContext.getMetaData(), binderContext.getCurrentDatabaseName(), binderContext.getHintValueContext(), binderContext.getSqlStatement());
        insertStatementBinderContext.getExternalTableBinderContexts().putAll(binderContext.getExternalTableBinderContexts());
        insertStatementBinderContext.getExternalTableBinderContexts().putAll(sourceTableBinderContexts);
        InsertStatement result = sqlStatement.getClass().getDeclaredConstructor().newInstance();
        result.setTable(tableSegment);
        sqlStatement.getInsertColumns()
                .ifPresent(optional -> result.setInsertColumns(InsertColumnsSegmentBinder.bind(sqlStatement.getInsertColumns().get(), binderContext, targetTableBinderContexts)));
        sqlStatement.getInsertSelect().ifPresent(result::setInsertSelect);
        Collection<InsertValuesSegment> insertValues = new LinkedList<>();
        Map<ParameterMarkerSegment, ColumnSegmentBoundInfo> parameterMarkerSegmentBoundInfos = new LinkedHashMap<>();
        List<ColumnSegment> columnSegments = new ArrayList<>(result.getInsertColumns().map(InsertColumnsSegment::getColumns)
                .orElseGet(() -> getVisibleColumns(targetTableBinderContexts.values().iterator().next().getProjectionSegments())));
        for (InsertValuesSegment each : sqlStatement.getValues()) {
            List<ExpressionSegment> values = new LinkedList<>();
            int index = 0;
            for (ExpressionSegment expression : each.getValues()) {
                values.add(ExpressionSegmentBinder.bind(expression, SegmentType.VALUES, insertStatementBinderContext, targetTableBinderContexts, sourceTableBinderContexts));
                if (expression instanceof ParameterMarkerSegment) {
                    parameterMarkerSegmentBoundInfos.put((ParameterMarkerSegment) expression, columnSegments.get(index).getColumnBoundInfo());
                }
                index++;
            }
            insertValues.add(new InsertValuesSegment(each.getStartIndex(), each.getStopIndex(), values));
        }
        result.getValues().addAll(insertValues);
        sqlStatement.getOnDuplicateKeyColumns().ifPresent(result::setOnDuplicateKeyColumns);
        sqlStatement.getSetAssignment().ifPresent(result::setSetAssignment);
        sqlStatement.getWithSegment().ifPresent(result::setWithSegment);
        sqlStatement.getOutputSegment().ifPresent(result::setOutputSegment);
        sqlStatement.getMultiTableInsertType().ifPresent(result::setMultiTableInsertType);
        sqlStatement.getMultiTableInsertIntoSegment().ifPresent(result::setMultiTableInsertIntoSegment);
        sqlStatement.getMultiTableConditionalIntoSegment().ifPresent(result::setMultiTableConditionalIntoSegment);
        sqlStatement.getReturningSegment().ifPresent(result::setReturningSegment);
        sqlStatement.getWhere().ifPresent(optional -> result.setWhere(WhereSegmentBinder.bind(optional, insertStatementBinderContext, targetTableBinderContexts, sourceTableBinderContexts)));
        result.addParameterMarkerSegments(ParameterMarkerSegmentBinder.bind(sqlStatement.getParameterMarkerSegments(), parameterMarkerSegmentBoundInfos));
        result.getCommentSegments().addAll(sqlStatement.getCommentSegments());
        return result;
    }
    
    private Collection<ColumnSegment> getVisibleColumns(final Collection<ProjectionSegment> projectionSegments) {
        Collection<ColumnSegment> result = new LinkedList<>();
        for (ProjectionSegment each : projectionSegments) {
            if (each instanceof ColumnProjectionSegment && each.isVisible()) {
                result.add(((ColumnProjectionSegment) each).getColumn());
            }
        }
        return result;
    }
    
    @SneakyThrows
    private UpdateStatement bindMergeUpdate(final UpdateStatement sqlStatement, final SimpleTableSegment tableSegment, final SQLStatementBinderContext binderContext,
                                            final Multimap<CaseInsensitiveString, TableSegmentBinderContext> targetTableBinderContexts,
                                            final Multimap<CaseInsensitiveString, TableSegmentBinderContext> sourceTableBinderContexts) {
        UpdateStatement result = sqlStatement.getClass().getDeclaredConstructor().newInstance();
        result.setTable(tableSegment);
        Collection<ColumnAssignmentSegment> assignments = new LinkedList<>();
        SQLStatementBinderContext updateStatementBinderContext =
                new SQLStatementBinderContext(binderContext.getMetaData(), binderContext.getCurrentDatabaseName(), binderContext.getHintValueContext(), binderContext.getSqlStatement());
        updateStatementBinderContext.getExternalTableBinderContexts().putAll(binderContext.getExternalTableBinderContexts());
        updateStatementBinderContext.getExternalTableBinderContexts().putAll(sourceTableBinderContexts);
        Map<ParameterMarkerSegment, ColumnSegmentBoundInfo> parameterMarkerSegmentBoundInfos = new LinkedHashMap<>(sqlStatement.getSetAssignment().getAssignments().size(), 1F);
        for (ColumnAssignmentSegment each : sqlStatement.getSetAssignment().getAssignments()) {
            List<ColumnSegment> columnSegments = new ArrayList<>(each.getColumns().size());
            each.getColumns().forEach(column -> columnSegments.add(
                    ColumnSegmentBinder.bind(column, SegmentType.SET_ASSIGNMENT, updateStatementBinderContext, targetTableBinderContexts, LinkedHashMultimap.create())));
            ExpressionSegment expression =
                    ExpressionSegmentBinder.bind(each.getValue(), SegmentType.SET_ASSIGNMENT, updateStatementBinderContext, targetTableBinderContexts, LinkedHashMultimap.create());
            ColumnAssignmentSegment columnAssignmentSegment = new ColumnAssignmentSegment(each.getStartIndex(), each.getStopIndex(), columnSegments, expression);
            assignments.add(columnAssignmentSegment);
            if (expression instanceof ParameterMarkerSegment) {
                parameterMarkerSegmentBoundInfos.put((ParameterMarkerSegment) expression, columnAssignmentSegment.getColumns().get(0).getColumnBoundInfo());
            }
        }
        SetAssignmentSegment setAssignmentSegment = new SetAssignmentSegment(sqlStatement.getSetAssignment().getStartIndex(), sqlStatement.getSetAssignment().getStopIndex(), assignments);
        result.setSetAssignment(setAssignmentSegment);
        sqlStatement.getWhere().ifPresent(optional -> result.setWhere(WhereSegmentBinder.bind(optional, updateStatementBinderContext, targetTableBinderContexts, LinkedHashMultimap.create())));
        sqlStatement.getDeleteWhere()
                .ifPresent(optional -> result.setDeleteWhere(WhereSegmentBinder.bind(optional, updateStatementBinderContext, targetTableBinderContexts, LinkedHashMultimap.create())));
        sqlStatement.getOrderBy().ifPresent(result::setOrderBy);
        sqlStatement.getLimit().ifPresent(result::setLimit);
        sqlStatement.getWithSegment().ifPresent(result::setWithSegment);
        result.addParameterMarkerSegments(ParameterMarkerSegmentBinder.bind(sqlStatement.getParameterMarkerSegments(), parameterMarkerSegmentBoundInfos));
        result.getCommentSegments().addAll(sqlStatement.getCommentSegments());
        return result;
    }
}
