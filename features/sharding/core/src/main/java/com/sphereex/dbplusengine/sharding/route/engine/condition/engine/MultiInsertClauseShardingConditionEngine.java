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

package com.sphereex.dbplusengine.sharding.route.engine.condition.engine;

import lombok.RequiredArgsConstructor;
import org.apache.shardingsphere.infra.algorithm.core.context.AlgorithmSQLContext;
import org.apache.shardingsphere.infra.binder.context.segment.insert.keygen.GeneratedKeyContext;
import org.apache.shardingsphere.infra.binder.context.segment.insert.values.InsertSelectContext;
import org.apache.shardingsphere.infra.binder.context.segment.insert.values.InsertValueContext;
import org.apache.shardingsphere.infra.binder.context.statement.dml.InsertStatementContext;
import org.apache.shardingsphere.infra.binder.context.statement.dml.SelectStatementContext;
import org.apache.shardingsphere.infra.database.core.type.DatabaseTypeRegistry;
import org.apache.shardingsphere.infra.exception.core.ShardingSpherePreconditions;
import org.apache.shardingsphere.infra.exception.dialect.exception.data.InsertColumnsAndValuesMismatchedException;
import org.apache.shardingsphere.infra.exception.dialect.exception.syntax.table.NoSuchTableException;
import org.apache.shardingsphere.infra.metadata.database.ShardingSphereDatabase;
import org.apache.shardingsphere.infra.metadata.database.schema.model.ShardingSphereSchema;
import org.apache.shardingsphere.sharding.route.engine.condition.ExpressionConditionUtils;
import org.apache.shardingsphere.sharding.route.engine.condition.ShardingCondition;
import org.apache.shardingsphere.sharding.route.engine.condition.engine.WhereClauseShardingConditionEngine;
import org.apache.shardingsphere.sharding.route.engine.condition.value.ListShardingConditionValue;
import org.apache.shardingsphere.sharding.rule.ShardingRule;
import org.apache.shardingsphere.sql.parser.statement.core.segment.dml.expr.ExpressionSegment;
import org.apache.shardingsphere.sql.parser.statement.core.segment.dml.expr.complex.CommonExpressionSegment;
import org.apache.shardingsphere.sql.parser.statement.core.segment.dml.expr.simple.LiteralExpressionSegment;
import org.apache.shardingsphere.sql.parser.statement.core.segment.dml.expr.simple.ParameterMarkerExpressionSegment;
import org.apache.shardingsphere.sql.parser.statement.core.segment.dml.expr.simple.SimpleExpressionSegment;
import org.apache.shardingsphere.sql.parser.statement.core.segment.generic.table.SimpleTableSegment;
import org.apache.shardingsphere.timeservice.core.rule.TimestampServiceRule;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;

/**
 * Sharding condition engine for multi insert clause.
 */
@RequiredArgsConstructor
public final class MultiInsertClauseShardingConditionEngine {
    
    private final ShardingSphereDatabase database;
    
    private final ShardingRule rule;
    
    private final TimestampServiceRule timestampServiceRule;
    
    /**
     * Create sharding conditions.
     *
     * @param sqlStatementContext SQL statement context
     * @param params SQL parameters
     * @return sharding conditions
     */
    public List<ShardingCondition> createShardingConditions(final InsertStatementContext sqlStatementContext, final List<Object> params) {
        List<ShardingCondition> result = null == sqlStatementContext.getInsertSelectContext() || isInsertSelectDualTable(sqlStatementContext.getInsertSelectContext())
                ? createShardingConditionsWithInsertValues(sqlStatementContext, params)
                : createShardingConditionsWithInsertSelect(sqlStatementContext, params);
        appendGeneratedKeyConditions(sqlStatementContext, result);
        return result;
    }
    
    private boolean isInsertSelectDualTable(final InsertSelectContext insertSelectContext) {
        return insertSelectContext.getSelectStatementContext().getSqlStatement().getFrom().filter(optional -> optional instanceof SimpleTableSegment)
                .map(optional -> "DUAL".equalsIgnoreCase(((SimpleTableSegment) optional).getTableName().getIdentifier().getValue())).orElse(false);
    }
    
    private List<ShardingCondition> createShardingConditionsWithInsertValues(final InsertStatementContext sqlStatementContext, final List<Object> params) {
        ShardingSpherePreconditions.checkState(!sqlStatementContext.getMultiInsertStatementContexts().isEmpty(), () -> new IllegalStateException("Can not get multi insert statement context!"));
        InsertStatementContext sampleInsertStatementContext = sqlStatementContext.getMultiInsertStatementContexts().iterator().next();
        String tableName = sampleInsertStatementContext.getSqlStatement().getTable().map(optional -> optional.getTableName().getIdentifier().getValue())
                .orElseGet(() -> sampleInsertStatementContext.getTablesContext().getTableNames().iterator().next());
        Collection<String> columnNames = getColumnNames(sampleInsertStatementContext);
        List<InsertValueContext> insertValueContexts = getInsertValueContexts(sqlStatementContext.getMultiInsertStatementContexts());
        List<ShardingCondition> result = new ArrayList<>(insertValueContexts.size());
        int rowNumber = 0;
        for (InsertValueContext each : insertValueContexts) {
            result.add(createShardingCondition(tableName, columnNames.iterator(), each, params, ++rowNumber));
        }
        appendMissingShardingConditions(sampleInsertStatementContext, columnNames, result);
        return result;
    }
    
    private void appendMissingShardingConditions(final InsertStatementContext sqlStatementContext, final Collection<String> columnNames, final List<ShardingCondition> shardingConditions) {
        String defaultSchemaName = new DatabaseTypeRegistry(sqlStatementContext.getDatabaseType()).getDefaultSchemaName(database.getName());
        ShardingSphereSchema schema = sqlStatementContext.getTablesContext().getSchemaName().map(database::getSchema).orElseGet(() -> database.getSchema(defaultSchemaName));
        String tableName = sqlStatementContext.getSqlStatement().getTable().map(optional -> optional.getTableName().getIdentifier().getValue())
                .orElseGet(() -> sqlStatementContext.getTablesContext().getTableNames().iterator().next());
        ShardingSpherePreconditions.checkState(schema.containsTable(tableName), () -> new NoSuchTableException(tableName));
        for (String each : schema.getTable(tableName).findColumnNamesIfNotExistedFrom(columnNames)) {
            if (!rule.isGenerateKeyColumn(each, tableName) && rule.findShardingColumn(each, tableName).isPresent()) {
                appendMissingShardingConditions(tableName, each, shardingConditions);
            }
        }
    }
    
    private void appendMissingShardingConditions(final String tableName, final String columnName, final List<ShardingCondition> shardingConditions) {
        for (ShardingCondition each : shardingConditions) {
            each.getValues().add(new ListShardingConditionValue<>(columnName, tableName, Collections.singletonList(null)));
        }
    }
    
    private Collection<String> getColumnNames(final InsertStatementContext insertStatementContext) {
        Optional<GeneratedKeyContext> generatedKey = insertStatementContext.getGeneratedKeyContext();
        List<String> columnNames = insertStatementContext.getColumnNames();
        if (generatedKey.isPresent() && generatedKey.get().isGenerated()) {
            Collection<String> result = new LinkedHashSet<>(columnNames);
            result.remove(generatedKey.get().getColumnName());
            return result;
        }
        return new LinkedHashSet<>(columnNames);
    }
    
    private List<InsertValueContext> getInsertValueContexts(final Collection<InsertStatementContext> insertStatementContexts) {
        List<InsertValueContext> result = new LinkedList<>();
        for (InsertStatementContext each : insertStatementContexts) {
            result.addAll(each.getInsertValueContexts());
        }
        return result;
    }
    
    private ShardingCondition createShardingCondition(final String tableName, final Iterator<String> columnNames,
                                                      final InsertValueContext insertValueContext, final List<Object> params, final int rowNumber) {
        ShardingCondition result = new ShardingCondition();
        for (ExpressionSegment each : insertValueContext.getValueExpressions()) {
            if (!columnNames.hasNext()) {
                throw new InsertColumnsAndValuesMismatchedException(rowNumber);
            }
            Optional<String> shardingColumn = rule.findShardingColumn(columnNames.next(), tableName);
            if (!shardingColumn.isPresent()) {
                continue;
            }
            if (each instanceof SimpleExpressionSegment) {
                List<Integer> parameterMarkerIndexes = each instanceof ParameterMarkerExpressionSegment
                        ? Collections.singletonList(((ParameterMarkerExpressionSegment) each).getParameterMarkerIndex())
                        : Collections.emptyList();
                Object shardingValue = getShardingValue((SimpleExpressionSegment) each, params);
                result.getValues().add(new ListShardingConditionValue<>(shardingColumn.get(), tableName, Collections.singletonList(shardingValue),
                        parameterMarkerIndexes));
            } else if (each instanceof CommonExpressionSegment) {
                generateShardingCondition((CommonExpressionSegment) each, result, shardingColumn.get(), tableName);
            } else if (ExpressionConditionUtils.isNowExpression(each)) {
                result.getValues().add(new ListShardingConditionValue<>(shardingColumn.get(), tableName, Collections.singletonList(timestampServiceRule.getTimestamp())));
            }
        }
        return result;
    }
    
    private void generateShardingCondition(final CommonExpressionSegment expressionSegment, final ShardingCondition condition, final String shardingColumn, final String tableName) {
        try {
            Integer value = Integer.valueOf(expressionSegment.getText());
            condition.getValues().add(new ListShardingConditionValue<>(shardingColumn, tableName, Collections.singletonList(value)));
        } catch (final NumberFormatException ex) {
            condition.getValues().add(new ListShardingConditionValue<>(shardingColumn, tableName, Collections.singletonList(expressionSegment.getText())));
        }
    }
    
    private Object getShardingValue(final SimpleExpressionSegment expressionSegment, final List<Object> params) {
        return expressionSegment instanceof ParameterMarkerExpressionSegment
                ? params.get(((ParameterMarkerExpressionSegment) expressionSegment).getParameterMarkerIndex())
                : ((LiteralExpressionSegment) expressionSegment).getLiterals();
    }
    
    private List<ShardingCondition> createShardingConditionsWithInsertSelect(final InsertStatementContext sqlStatementContext, final List<Object> params) {
        SelectStatementContext selectStatementContext = sqlStatementContext.getInsertSelectContext().getSelectStatementContext();
        return new LinkedList<>(new WhereClauseShardingConditionEngine(rule, timestampServiceRule).createShardingConditions(selectStatementContext, params));
    }
    
    private void appendGeneratedKeyConditions(final InsertStatementContext sqlStatementContext, final List<ShardingCondition> shardingConditions) {
        int index = 0;
        for (InsertStatementContext each : sqlStatementContext.getMultiInsertStatementContexts()) {
            Optional<GeneratedKeyContext> generatedKey = each.getGeneratedKeyContext();
            String tableName = each.getSqlStatement().getTable().map(optional -> optional.getTableName().getIdentifier().getValue()).orElse("");
            if (generatedKey.isPresent() && generatedKey.get().isGenerated() && rule.findShardingTable(tableName).isPresent()) {
                String schemaName = each.getTablesContext().getSchemaName()
                        .orElseGet(() -> new DatabaseTypeRegistry(each.getDatabaseType()).getDefaultSchemaName(database.getName()));
                AlgorithmSQLContext algorithmSQLContext = new AlgorithmSQLContext(database.getName(), schemaName, tableName, generatedKey.get().getColumnName());
                generatedKey.get().getGeneratedValues().addAll(rule.generateKeys(algorithmSQLContext, each.getValueListCount()));
                generatedKey.get().setSupportAutoIncrement(rule.isSupportAutoIncrement(tableName));
                if (rule.findShardingColumn(generatedKey.get().getColumnName(), tableName).isPresent()) {
                    appendGeneratedKeyCondition(generatedKey.get(), tableName, shardingConditions.get(index++));
                }
            }
        }
    }
    
    private void appendGeneratedKeyCondition(final GeneratedKeyContext generatedKey, final String tableName, final ShardingCondition shardingCondition) {
        Iterator<Comparable<?>> generatedValuesIterator = generatedKey.getGeneratedValues().iterator();
        shardingCondition.getValues().add(new ListShardingConditionValue<>(generatedKey.getColumnName(), tableName, Collections.<Comparable<?>>singletonList(generatedValuesIterator.next())));
    }
}
