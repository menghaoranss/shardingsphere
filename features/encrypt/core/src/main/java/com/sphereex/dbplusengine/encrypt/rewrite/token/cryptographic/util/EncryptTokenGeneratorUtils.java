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

package com.sphereex.dbplusengine.encrypt.rewrite.token.cryptographic.util;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.apache.shardingsphere.encrypt.rule.EncryptRule;
import org.apache.shardingsphere.encrypt.rule.column.EncryptColumn;
import org.apache.shardingsphere.infra.annotation.HighFrequencyInvocation;
import org.apache.shardingsphere.infra.binder.context.segment.select.groupby.GroupByContext;
import org.apache.shardingsphere.infra.binder.context.segment.select.orderby.OrderByItem;
import org.apache.shardingsphere.infra.binder.context.segment.select.projection.Projection;
import org.apache.shardingsphere.infra.binder.context.segment.select.projection.impl.ExpressionProjection;
import org.apache.shardingsphere.infra.binder.context.segment.select.projection.impl.ShorthandProjection;
import org.apache.shardingsphere.infra.binder.context.statement.SQLStatementContext;
import org.apache.shardingsphere.infra.binder.context.statement.dml.SelectStatementContext;
import org.apache.shardingsphere.infra.binder.context.type.WhereAvailable;
import org.apache.shardingsphere.infra.database.oracle.type.OracleDatabaseType;
import org.apache.shardingsphere.sql.parser.statement.core.segment.dml.column.ColumnSegment;
import org.apache.shardingsphere.sql.parser.statement.core.segment.dml.item.ExpressionProjectionSegment;
import org.apache.shardingsphere.sql.parser.statement.core.segment.dml.order.item.ColumnOrderByItemSegment;
import org.apache.shardingsphere.sql.parser.statement.core.segment.dml.predicate.WhereSegment;
import org.apache.shardingsphere.sql.parser.statement.core.segment.generic.table.JoinTableSegment;
import org.apache.shardingsphere.sql.parser.statement.core.segment.generic.table.TableSegment;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Encrypt token generator utils.
 */
@HighFrequencyInvocation
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class EncryptTokenGeneratorUtils {
    
    /**
     * Judge whether SQL statement needs rewrite using and natural join or not.
     *
     * @param selectStatementContext select statement context
     * @param rule encrypt rule
     * @param databaseEncryptRules database encrypt rules
     * @return whether SQL statement needs rewrite using and natural join or not
     */
    public static boolean isNeedRewriteUsingNaturalJoin(final SelectStatementContext selectStatementContext, final EncryptRule rule, final Map<String, EncryptRule> databaseEncryptRules) {
        TableSegment tableSegment = selectStatementContext.getSqlStatement().getFrom().orElse(null);
        if (!(tableSegment instanceof JoinTableSegment)) {
            return false;
        }
        Collection<Projection> projections = selectStatementContext.getProjectionsContext().getProjections();
        return isUsingColumnContainsEncryptColumn((JoinTableSegment) tableSegment, rule, databaseEncryptRules)
                || selectStatementContext.getDatabaseType() instanceof OracleDatabaseType && containsShorthandProjection(projections) && countUsing((JoinTableSegment) tableSegment) > 1;
    }
    
    private static boolean isUsingColumnContainsEncryptColumn(final JoinTableSegment joinTableSegment, final EncryptRule rule, final Map<String, EncryptRule> databaseEncryptRules) {
        return isUsingColumnContainsEncryptColumn(joinTableSegment.getUsing(), joinTableSegment.getLeft(), joinTableSegment.getRight(), rule, databaseEncryptRules)
                || isUsingColumnContainsEncryptColumn(joinTableSegment.getDerivedUsing(), joinTableSegment.getLeft(), joinTableSegment.getRight(), rule, databaseEncryptRules);
    }
    
    private static boolean isUsingColumnContainsEncryptColumn(final Collection<ColumnSegment> usingColumns, final TableSegment leftTable, final TableSegment rightTable, final EncryptRule rule,
                                                              final Map<String, EncryptRule> databaseEncryptRules) {
        for (ColumnSegment each : usingColumns) {
            String tableName = each.getColumnBoundInfo().getOriginalTable().getValue();
            String columnName = each.getColumnBoundInfo().getOriginalColumn().getValue();
            EncryptRule usedEncryptRule = databaseEncryptRules.getOrDefault(each.getColumnBoundInfo().getOriginalDatabase().getValue(), rule);
            if (usedEncryptRule.findEncryptTable(tableName).isPresent() && usedEncryptRule.getEncryptTable(tableName).isEncryptColumn(columnName)) {
                return true;
            }
            if (leftTable instanceof JoinTableSegment && isUsingColumnContainsEncryptColumn((JoinTableSegment) leftTable, rule, databaseEncryptRules)
                    || rightTable instanceof JoinTableSegment && isUsingColumnContainsEncryptColumn((JoinTableSegment) rightTable, rule, databaseEncryptRules)) {
                return true;
            }
        }
        return false;
    }
    
    private static boolean containsShorthandProjection(final Collection<Projection> projections) {
        for (Projection each : projections) {
            if (each instanceof ShorthandProjection) {
                return true;
            }
        }
        return false;
    }
    
    private static int countUsing(final JoinTableSegment joinTableSegment) {
        int result = 0;
        if (!joinTableSegment.getUsing().isEmpty()) {
            result++;
        }
        if (joinTableSegment.getLeft() instanceof JoinTableSegment) {
            result += countUsing((JoinTableSegment) joinTableSegment.getLeft());
        }
        if (joinTableSegment.getRight() instanceof JoinTableSegment) {
            result += countUsing((JoinTableSegment) joinTableSegment.getRight());
        }
        return result;
    }
    
    /**
     * Judge whether contains using column or not.
     *
     * @param columnSegment column segment
     * @param tableSegment table segment
     * @return whether contains using column or not
     */
    public static boolean containsUsingColumn(final ColumnSegment columnSegment, final TableSegment tableSegment) {
        if (columnSegment.getOwner().isPresent() || !(tableSegment instanceof JoinTableSegment)) {
            return false;
        }
        if (containsUsingColumn(columnSegment, ((JoinTableSegment) tableSegment).getUsing()) || containsUsingColumn(columnSegment, ((JoinTableSegment) tableSegment).getDerivedUsing())) {
            return true;
        }
        return containsUsingColumn(columnSegment, ((JoinTableSegment) tableSegment).getLeft()) || containsUsingColumn(columnSegment, ((JoinTableSegment) tableSegment).getRight());
    }
    
    private static boolean containsUsingColumn(final ColumnSegment columnSegment, final Collection<ColumnSegment> usingColumns) {
        for (ColumnSegment each : usingColumns) {
            if (each.getIdentifier().getValue().equalsIgnoreCase(columnSegment.getIdentifier().getValue())) {
                return true;
            }
        }
        return false;
    }
    
    /**
     * Judge whether encrypt column contains in group by item or not.
     *
     * @param columnSegment column segment
     * @param encryptColumn encrypt column
     * @param sqlStatementContext SQL statement context
     * @return whether encrypt column contains in group by item or not
     */
    public static boolean isEncryptColumnContainsInGroupByItem(final ColumnSegment columnSegment, final EncryptColumn encryptColumn, final SQLStatementContext sqlStatementContext) {
        if (!(sqlStatementContext instanceof SelectStatementContext)) {
            return false;
        }
        if (encryptColumn.getPlain().isPresent() && encryptColumn.getPlain().get().isQueryWithPlain()) {
            return false;
        }
        Optional<GroupByContext> groupByContext = findGroupByContext((SelectStatementContext) sqlStatementContext, columnSegment);
        if (!groupByContext.isPresent()) {
            return false;
        }
        for (OrderByItem each : groupByContext.get().getItems()) {
            if (!(each.getSegment() instanceof ColumnOrderByItemSegment)) {
                continue;
            }
            ColumnSegment groupByColumn = ((ColumnOrderByItemSegment) each.getSegment()).getColumn();
            if (groupByColumn.getIdentifier().getValue().equalsIgnoreCase(columnSegment.getIdentifier().getValue())) {
                return true;
            }
        }
        return false;
    }
    
    private static Optional<GroupByContext> findGroupByContext(final SelectStatementContext sqlStatementContext, final ColumnSegment columnSegment) {
        Optional<GroupByContext> result = findGroupByContextByOrderBy(sqlStatementContext, columnSegment);
        if (result.isPresent()) {
            return result;
        }
        result = findGroupByContextByHaving(sqlStatementContext, columnSegment);
        if (result.isPresent()) {
            return result;
        }
        for (SelectStatementContext each : sqlStatementContext.getSubqueryContexts().values()) {
            result = findGroupByContext(each, columnSegment);
            if (result.isPresent()) {
                return result;
            }
        }
        return Optional.empty();
    }
    
    private static Optional<GroupByContext> findGroupByContextByOrderBy(final SelectStatementContext sqlStatementContext, final ColumnSegment columnSegment) {
        if (!sqlStatementContext.getOrderByContext().getItems().isEmpty()) {
            List<OrderByItem> orderByItems = new ArrayList<>(sqlStatementContext.getOrderByContext().getItems());
            if (orderByItems.get(0).getSegment().getStartIndex() <= columnSegment.getStartIndex()
                    && orderByItems.get(orderByItems.size() - 1).getSegment().getStopIndex() >= columnSegment.getStopIndex()) {
                return Optional.of(sqlStatementContext.getGroupByContext());
            }
        }
        return Optional.empty();
    }
    
    private static Optional<GroupByContext> findGroupByContextByHaving(final SelectStatementContext sqlStatementContext, final ColumnSegment columnSegment) {
        if (sqlStatementContext.getSqlStatement().getHaving().isPresent()) {
            if (sqlStatementContext.getSqlStatement().getHaving().get().getStartIndex() <= columnSegment.getStartIndex()
                    && sqlStatementContext.getSqlStatement().getHaving().get().getStopIndex() >= columnSegment.getStopIndex()) {
                return Optional.of(sqlStatementContext.getGroupByContext());
            }
        }
        return Optional.empty();
    }
    
    /**
     * Get projection compare operator where segments.
     *
     * @param sqlStatementContext SQL statement context
     * @param allSubqueryContexts all subquery contexts
     * @return projection compare operator where segments
     */
    public static Collection<WhereSegment> getProjectionCompareOperatorWhereSegments(final WhereAvailable sqlStatementContext,
                                                                                     final Collection<SelectStatementContext> allSubqueryContexts) {
        Collection<WhereSegment> result = new LinkedList<>();
        if (sqlStatementContext instanceof SelectStatementContext) {
            result.addAll(getProjectionCompareOperatorWhereSegments(null, Collections.singleton((SelectStatementContext) sqlStatementContext)));
        }
        for (SelectStatementContext each : allSubqueryContexts) {
            for (Projection projection : each.getProjectionsContext().getProjections()) {
                if (projection instanceof ExpressionProjection) {
                    ExpressionProjectionSegment expressionSegment = ((ExpressionProjection) projection).getExpressionSegment();
                    result.add(new WhereSegment(expressionSegment.getStartIndex(), expressionSegment.getStopIndex(), expressionSegment.getExpr()));
                }
            }
        }
        return result;
    }
}
