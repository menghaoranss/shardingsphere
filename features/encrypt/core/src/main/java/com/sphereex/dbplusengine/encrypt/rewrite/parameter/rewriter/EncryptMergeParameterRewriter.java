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

import com.sphereex.dbplusengine.infra.binder.context.statement.dml.MergeStatementContext;
import lombok.RequiredArgsConstructor;
import org.apache.shardingsphere.encrypt.rewrite.condition.EncryptCondition;
import org.apache.shardingsphere.encrypt.rewrite.condition.EncryptConditionEngine;
import org.apache.shardingsphere.encrypt.rewrite.parameter.rewriter.EncryptAssignmentParameterRewriter;
import org.apache.shardingsphere.encrypt.rewrite.parameter.rewriter.EncryptInsertValueParameterRewriter;
import org.apache.shardingsphere.encrypt.rewrite.parameter.rewriter.EncryptPredicateParameterRewriter;
import org.apache.shardingsphere.encrypt.rule.EncryptRule;
import org.apache.shardingsphere.infra.binder.context.statement.SQLStatementContext;
import org.apache.shardingsphere.infra.binder.context.statement.dml.SelectStatementContext;
import org.apache.shardingsphere.infra.binder.context.statement.dml.UpdateStatementContext;
import org.apache.shardingsphere.infra.hint.HintValueContext;
import org.apache.shardingsphere.infra.metadata.database.ShardingSphereDatabase;
import org.apache.shardingsphere.infra.rewrite.parameter.builder.ParameterBuilder;
import org.apache.shardingsphere.infra.rewrite.parameter.builder.impl.GroupedParameterBuilder;
import org.apache.shardingsphere.infra.rewrite.parameter.builder.impl.StandardParameterBuilder;
import org.apache.shardingsphere.infra.rewrite.parameter.rewriter.ParameterRewriter;
import org.apache.shardingsphere.sql.parser.statement.core.extractor.ColumnExtractor;
import org.apache.shardingsphere.sql.parser.statement.core.extractor.ExpressionExtractor;
import org.apache.shardingsphere.sql.parser.statement.core.segment.dml.column.ColumnSegment;
import org.apache.shardingsphere.sql.parser.statement.core.segment.dml.expr.ExpressionWithParamsSegment;
import org.apache.shardingsphere.sql.parser.statement.core.segment.dml.predicate.WhereSegment;
import org.apache.shardingsphere.sql.parser.statement.core.statement.dml.InsertStatement;
import org.apache.shardingsphere.sql.parser.statement.core.statement.dml.MergeStatement;
import org.apache.shardingsphere.sql.parser.statement.oracle.dml.OracleInsertStatement;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Merge parameter rewriter for encrypt.
 */
@RequiredArgsConstructor
public final class EncryptMergeParameterRewriter implements ParameterRewriter {
    
    private final EncryptRule rule;
    
    private final Map<String, EncryptRule> databaseEncryptRules;
    
    private final String databaseName;
    
    private final ShardingSphereDatabase database;
    
    private final HintValueContext hintValueContext;
    
    @Override
    public boolean isNeedRewrite(final SQLStatementContext sqlStatementContext) {
        return sqlStatementContext instanceof MergeStatementContext;
    }
    
    @Override
    public void rewrite(final ParameterBuilder paramBuilder, final SQLStatementContext sqlStatementContext, final List<Object> params) {
        MergeStatement mergeStatement = (MergeStatement) sqlStatementContext.getSqlStatement();
        int parameterOffset = 0;
        if (((MergeStatementContext) sqlStatementContext).getSourceSubQueryStatement().isPresent()) {
            parameterOffset = rewriteSubQuery((StandardParameterBuilder) paramBuilder, (MergeStatementContext) sqlStatementContext, params, parameterOffset);
        }
        parameterOffset = rewriteOnExpression((StandardParameterBuilder) paramBuilder, sqlStatementContext, params, mergeStatement, parameterOffset);
        if (null != ((MergeStatementContext) sqlStatementContext).getUpdateStatementContext()) {
            parameterOffset = rewriteUpdate(paramBuilder, (MergeStatementContext) sqlStatementContext, params, mergeStatement, parameterOffset);
        }
        if (null != ((MergeStatementContext) sqlStatementContext).getInsertStatementContext()) {
            rewriteInsert((StandardParameterBuilder) paramBuilder, (MergeStatementContext) sqlStatementContext, params, parameterOffset);
        }
    }
    
    private int rewriteSubQuery(final StandardParameterBuilder paramBuilder, final MergeStatementContext sqlStatementContext, final List<Object> params, final int parameterOffset) {
        SelectStatementContext select = sqlStatementContext.getSourceSubQueryStatement().get();
        Collection<EncryptCondition> encryptConditions = new EncryptConditionEngine(rule, databaseEncryptRules).createEncryptConditions(select.getWhereSegments());
        EncryptPredicateParameterRewriter predicateParameterRewriter = new EncryptPredicateParameterRewriter(rule, databaseName, encryptConditions, hintValueContext);
        if (!predicateParameterRewriter.isNeedRewrite(select)) {
            return parameterOffset;
        }
        predicateParameterRewriter.rewrite(paramBuilder, select, params);
        return parameterOffset + select.getSqlStatement().getParameterMarkerSegments().size();
    }
    
    private int rewriteUpdate(final ParameterBuilder paramBuilder, final MergeStatementContext sqlStatementContext, final List<Object> params,
                              final MergeStatement mergeStatement, final int parameterOffset) {
        EncryptAssignmentParameterRewriter assignmentParameterRewriter = new EncryptAssignmentParameterRewriter(rule, databaseName, hintValueContext);
        assignmentParameterRewriter.rewrite(paramBuilder, sqlStatementContext.getUpdateStatementContext(), params);
        Collection<WhereSegment> whereSegments = new LinkedList<>();
        UpdateStatementContext updateStatementContext = sqlStatementContext.getUpdateStatementContext();
        updateStatementContext.getSqlStatement().getWhere().ifPresent(whereSegments::add);
        updateStatementContext.getSqlStatement().getDeleteWhere().ifPresent(whereSegments::add);
        Collection<ColumnSegment> columnSegments = new LinkedList<>();
        ColumnExtractor.extractColumnSegments(columnSegments, whereSegments);
        Collection<EncryptCondition> encryptConditions = new EncryptConditionEngine(rule, databaseEncryptRules).createEncryptConditions(whereSegments);
        EncryptPredicateParameterRewriter predicateParameterRewriter = new EncryptPredicateParameterRewriter(rule, databaseName, encryptConditions, hintValueContext);
        if (predicateParameterRewriter.isNeedRewrite(updateStatementContext)) {
            predicateParameterRewriter.rewrite(paramBuilder, updateStatementContext, params);
            return parameterOffset + mergeStatement.getUpdate().map(each -> each.getParameterMarkerSegments().size()).orElse(0);
        }
        return parameterOffset;
    }
    
    private void rewriteInsert(final StandardParameterBuilder paramBuilder, final MergeStatementContext sqlStatementContext, final List<Object> params, final int parameterOffset) {
        EncryptInsertValueParameterRewriter parameterRewriter = new EncryptInsertValueParameterRewriter(rule, databaseName);
        int insertWhereParamsCount = sqlStatementContext.getInsertStatementContext().getSqlStatement().getWhere()
                .map(optional -> ExpressionExtractor.getParameterMarkerExpressions(Collections.singleton(optional.getExpr())).size()).orElse(0);
        List<Object> insertParams = params.subList(parameterOffset,
                parameterOffset + sqlStatementContext.getInsertStatementContext().getSqlStatement().getParameterMarkerSegments().size() - insertWhereParamsCount);
        GroupedParameterBuilder insertParamsBuilder = new GroupedParameterBuilder(sqlStatementContext.getInsertStatementContext().getGroupedParameters(),
                sqlStatementContext.getInsertStatementContext().getOnDuplicateKeyUpdateParameters());
        parameterRewriter.rewrite(insertParamsBuilder, sqlStatementContext.getInsertStatementContext(), insertParams);
        mergeToParamBuilder(insertParamsBuilder, paramBuilder, parameterOffset);
        InsertStatement insertStatement = sqlStatementContext.getSqlStatement().getInsert().orElseThrow(() -> new IllegalStateException("Can not find merge insert statement."));
        if (insertStatement instanceof OracleInsertStatement && insertStatement.getWhere().isPresent()) {
            Collection<WhereSegment> whereSegments = new LinkedList<>();
            insertStatement.getWhere().ifPresent(whereSegments::add);
            Collection<ColumnSegment> columnSegments = new LinkedList<>();
            ColumnExtractor.extractColumnSegments(columnSegments, whereSegments);
            Collection<EncryptCondition> encryptConditions = new EncryptConditionEngine(rule, databaseEncryptRules).createEncryptConditions(whereSegments);
            EncryptPredicateParameterRewriter predicateParameterRewriter = new EncryptPredicateParameterRewriter(rule, databaseName, encryptConditions, hintValueContext);
            predicateParameterRewriter.rewrite(paramBuilder, sqlStatementContext, params);
        }
    }
    
    private void mergeToParamBuilder(final GroupedParameterBuilder insertParamBuilder, final StandardParameterBuilder parameterBuilder, final int parameterOffset) {
        int paramCount = 0;
        for (StandardParameterBuilder each : insertParamBuilder.getParameterBuilders()) {
            for (Entry<Integer, Object> entry : each.getReplacedIndexAndParameters().entrySet()) {
                parameterBuilder.addReplacedParameters(entry.getKey() + parameterOffset + paramCount, entry.getValue());
            }
            for (Entry<Integer, Collection<Object>> entry : each.getAddedIndexAndParameters().entrySet()) {
                parameterBuilder.addAddedParameters(entry.getKey() + parameterOffset + paramCount, entry.getValue());
            }
            paramCount += each.getParameters().size();
        }
        for (Entry<Integer, Object> entry : insertParamBuilder.getGenericParameterBuilder().getReplacedIndexAndParameters().entrySet()) {
            parameterBuilder.addReplacedParameters(entry.getKey() + parameterOffset + paramCount, entry.getValue());
        }
        for (Entry<Integer, Collection<Object>> entry : insertParamBuilder.getGenericParameterBuilder().getAddedIndexAndParameters().entrySet()) {
            parameterBuilder.addAddedParameters(entry.getKey() + parameterOffset + paramCount, entry.getValue());
        }
    }
    
    private int rewriteOnExpression(final StandardParameterBuilder paramBuilder, final SQLStatementContext sqlStatementContext, final List<Object> params,
                                    final MergeStatement mergeStatement, final int parameterOffset) {
        ExpressionWithParamsSegment expression = ((MergeStatement) sqlStatementContext.getSqlStatement()).getExpression();
        Collection<ColumnSegment> columnSegments = new LinkedList<>();
        ColumnExtractor.extractColumnSegments(columnSegments, Collections.singleton(new WhereSegment(0, 0, expression.getExpr())));
        Collection<EncryptCondition> conditions = new EncryptConditionEngine(rule, databaseEncryptRules)
                .createEncryptConditions(Collections.singleton(new WhereSegment(expression.getStartIndex(), expression.getStopIndex(), expression.getExpr())));
        EncryptPredicateParameterRewriter predicateParameterRewriter = new EncryptPredicateParameterRewriter(rule, databaseName, conditions, hintValueContext);
        predicateParameterRewriter.rewrite(paramBuilder, sqlStatementContext, params);
        return parameterOffset + mergeStatement.getExpression().getParameterMarkerSegments().size();
    }
}
