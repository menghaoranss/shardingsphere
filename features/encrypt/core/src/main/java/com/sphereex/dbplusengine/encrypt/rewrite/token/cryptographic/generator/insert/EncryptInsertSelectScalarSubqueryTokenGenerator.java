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

import com.sphereex.dbplusengine.encrypt.rewrite.aware.ConfigurationPropertiesAware;
import com.sphereex.dbplusengine.encrypt.rewrite.aware.SQLAware;
import com.sphereex.dbplusengine.encrypt.rewrite.token.cryptographic.pojo.EncryptScalarSubqueryAttachableToken;
import com.sphereex.dbplusengine.infra.rewrite.token.pojo.generic.ComposableSQLToken;
import com.sphereex.dbplusengine.infra.rewrite.token.pojo.generic.ComposableSQLTokenAvailable;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import org.apache.shardingsphere.encrypt.rewrite.aware.EncryptConditionsAware;
import org.apache.shardingsphere.encrypt.rewrite.condition.EncryptCondition;
import org.apache.shardingsphere.encrypt.rewrite.token.EncryptTokenGenerateBuilder;
import org.apache.shardingsphere.encrypt.rule.EncryptRule;
import org.apache.shardingsphere.encrypt.rule.table.EncryptTable;
import org.apache.shardingsphere.infra.binder.context.segment.select.projection.Projection;
import org.apache.shardingsphere.infra.binder.context.segment.select.projection.impl.ColumnProjection;
import org.apache.shardingsphere.infra.binder.context.segment.select.projection.impl.SubqueryProjection;
import org.apache.shardingsphere.infra.binder.context.statement.SQLStatementContext;
import org.apache.shardingsphere.infra.binder.context.statement.dml.InsertStatementContext;
import org.apache.shardingsphere.infra.binder.context.statement.dml.SelectStatementContext;
import org.apache.shardingsphere.infra.config.props.ConfigurationProperties;
import org.apache.shardingsphere.infra.exception.core.ShardingSpherePreconditions;
import org.apache.shardingsphere.infra.rewrite.context.SQLRewriteContext;
import org.apache.shardingsphere.infra.rewrite.sql.token.common.generator.CollectionSQLTokenGenerator;
import org.apache.shardingsphere.infra.rewrite.sql.token.common.generator.SQLTokenGenerators;
import org.apache.shardingsphere.infra.rewrite.sql.token.common.generator.aware.ConnectionContextAware;
import org.apache.shardingsphere.infra.rewrite.sql.token.common.generator.aware.PreviousSQLTokensAware;
import org.apache.shardingsphere.infra.rewrite.sql.token.common.pojo.SQLToken;
import org.apache.shardingsphere.infra.rewrite.sql.token.common.pojo.generic.SubstitutableColumnNameToken;
import org.apache.shardingsphere.infra.session.connection.ConnectionContext;
import org.apache.shardingsphere.sql.parser.statement.core.segment.dml.item.SubqueryProjectionSegment;
import org.apache.shardingsphere.sql.parser.statement.core.segment.generic.AliasSegment;
import org.apache.shardingsphere.sql.parser.statement.core.segment.generic.bound.ColumnSegmentBoundInfo;
import org.apache.shardingsphere.sql.parser.statement.core.value.identifier.IdentifierValue;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Insert select scalar subquery token generator for encrypt.
 */
@RequiredArgsConstructor
@Setter
public final class EncryptInsertSelectScalarSubqueryTokenGenerator
        implements
            CollectionSQLTokenGenerator<InsertStatementContext>,
            EncryptConditionsAware,
            SQLAware,
            ConnectionContextAware,
            ConfigurationPropertiesAware,
            PreviousSQLTokensAware {
    
    private final EncryptRule rule;
    
    private final Map<String, EncryptRule> databaseEncryptRules;
    
    private final SQLRewriteContext sqlRewriteContext;
    
    private Collection<EncryptCondition> encryptConditions;
    
    private ConnectionContext connectionContext;
    
    private ConfigurationProperties props;
    
    private List<SQLToken> previousSQLTokens;
    
    @Override
    public boolean isGenerateSQLToken(final SQLStatementContext sqlStatementContext) {
        return sqlStatementContext instanceof InsertStatementContext && null != ((InsertStatementContext) sqlStatementContext).getInsertSelectContext()
                && ((InsertStatementContext) sqlStatementContext).getSqlStatement().getTable().isPresent()
                && containsScalarSubquery(((InsertStatementContext) sqlStatementContext).getInsertSelectContext().getSelectStatementContext());
    }
    
    private boolean containsScalarSubquery(final SelectStatementContext selectStatementContext) {
        for (Projection each : selectStatementContext.getProjectionsContext().getProjections()) {
            if (each instanceof SubqueryProjection) {
                return true;
            }
        }
        return false;
    }
    
    @Override
    public Collection<SQLToken> generateSQLTokens(final InsertStatementContext insertStatementContext) {
        ShardingSpherePreconditions.checkNotNull(insertStatementContext.getInsertSelectContext(), () -> new IllegalStateException("Insert select segment is required."));
        SelectStatementContext selectStatementContext = insertStatementContext.getInsertSelectContext().getSelectStatementContext();
        Collection<SQLToken> result = new LinkedList<>();
        for (Projection each : insertStatementContext.getInsertSelectContext().getSelectStatementContext().getProjectionsContext().getProjections()) {
            if (!(each instanceof SubqueryProjection) || !selectStatementContext.getSubqueryContexts().containsKey(((SubqueryProjection) each).getSubquerySegment().getStartIndex())) {
                continue;
            }
            SubqueryProjectionSegment scalarSubquerySegment = ((SubqueryProjection) each).getSubquerySegment();
            SelectStatementContext scalarSubqueryContext = selectStatementContext.getSubqueryContexts().get(scalarSubquerySegment.getStartIndex());
            ColumnSegmentBoundInfo columnBoundedInfo = getColumnSegmentBoundInfo(scalarSubqueryContext.getProjectionsContext().getProjections().iterator().next());
            EncryptRule encryptRule = Optional.ofNullable(columnBoundedInfo.getOriginalDatabase()).map(IdentifierValue::getValue).map(databaseEncryptRules::get).orElse(this.rule);
            Optional<EncryptTable> encryptTable = encryptRule.findEncryptTable(columnBoundedInfo.getOriginalTable().getValue());
            if (!encryptTable.isPresent() || !encryptTable.get().isEncryptColumn(columnBoundedInfo.getOriginalColumn().getValue())) {
                continue;
            }
            SQLTokenGenerators sqlTokenGenerators = new SQLTokenGenerators();
            sqlTokenGenerators.addAll(new EncryptTokenGenerateBuilder(scalarSubqueryContext, encryptConditions, rule, databaseEncryptRules, sqlRewriteContext, props).getSQLTokenGenerators());
            List<SQLToken> sqlTokens = sqlTokenGenerators.generateSQLTokens(sqlRewriteContext.getDatabase(), scalarSubqueryContext, sqlRewriteContext.getParameters(), connectionContext);
            result.addAll(generateScalarSubquerySQLTokens(sqlTokens, scalarSubquerySegment));
        }
        return result;
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
    
    private Collection<SQLToken> generateScalarSubquerySQLTokens(final List<SQLToken> sqlTokens, final SubqueryProjectionSegment subqueryProjection) {
        Collection<SQLToken> result = new LinkedList<>();
        Collection<SQLToken> remainingSQLTokens = new LinkedList<>();
        Collection<SQLToken> nonEncryptRuleGeneratedSubqueryTokens = getNonEncryptRuleGeneratedSubqueryTokens(sqlTokens, subqueryProjection);
        for (SQLToken each : getUniqueSortedSQLTokens(sqlTokens)) {
            if (isContainsMultiProjectionsSubstitutableToken(each) && containsInScalarSubquery(subqueryProjection, each)) {
                result.addAll(generateScalarSubquerySQLTokens((SubstitutableColumnNameToken) each, subqueryProjection, nonEncryptRuleGeneratedSubqueryTokens));
            } else {
                remainingSQLTokens.add(each);
            }
        }
        Collection<ComposableSQLToken> composableSQLTokens = getComposableSQLTokens(result);
        for (SQLToken each : remainingSQLTokens) {
            appendRemainingSQLToken(composableSQLTokens, each, result);
        }
        return result;
    }
    
    private Collection<SQLToken> generateScalarSubquerySQLTokens(final SubstitutableColumnNameToken sqlToken, final SubqueryProjectionSegment subqueryProjection,
                                                                 final Collection<SQLToken> containedInSubqueryPreviousTokens) {
        Collection<SQLToken> result = new LinkedList<>();
        int index = 0;
        for (Projection each : sqlToken.getProjections()) {
            int stopIndex = subqueryProjection.getAliasSegment().map(AliasSegment::getStopIndex).orElseGet(subqueryProjection::getStopIndex);
            ComposableSQLToken composableSQLToken = new ComposableSQLToken(subqueryProjection.getStartIndex(), stopIndex);
            SQLToken scalarSubqueryToken = 0 == index ? composableSQLToken : new EncryptScalarSubqueryAttachableToken(stopIndex + 1, stopIndex + 1, composableSQLToken);
            SubstitutableColumnNameToken columnNameToken = new SubstitutableColumnNameToken(sqlToken.getStartIndex(),
                    sqlToken.getStopIndex(), Collections.singleton(each), sqlToken.getDatabaseType(), sqlRewriteContext.getDatabase(), sqlRewriteContext.getMetaData());
            composableSQLToken.getSqlTokens().add(columnNameToken);
            composableSQLToken.getSqlTokens().addAll(containedInSubqueryPreviousTokens);
            result.add(scalarSubqueryToken);
            index++;
        }
        return result;
    }
    
    private List<SQLToken> getUniqueSortedSQLTokens(final List<SQLToken> sqlTokens) {
        List<SQLToken> result = new LinkedList<>();
        for (SQLToken each : sqlTokens) {
            result.removeIf(token -> token.getStartIndex() == each.getStartIndex() && token.getStopIndex() == each.getStopIndex());
            result.add(each);
        }
        Collections.sort(result);
        return result;
    }
    
    private Collection<SQLToken> getNonEncryptRuleGeneratedSubqueryTokens(final List<SQLToken> sqlTokens, final SubqueryProjectionSegment subqueryProjection) {
        Collection<String> tokenStartStopIndexes = getTokenStartStopIndexes(sqlTokens);
        Collection<SQLToken> result = new LinkedList<>();
        for (SQLToken each : new LinkedList<>(previousSQLTokens)) {
            if (subqueryProjection.getStartIndex() <= each.getStartIndex() && subqueryProjection.getStopIndex() >= each.getStopIndex()) {
                result.add(each);
                previousSQLTokens.remove(each);
            }
        }
        result.removeIf(each -> tokenStartStopIndexes.contains(each.getStartIndex() + "-" + each.getStopIndex()));
        return result;
    }
    
    private Collection<String> getTokenStartStopIndexes(final List<SQLToken> sqlTokens) {
        Collection<String> result = new LinkedList<>();
        for (SQLToken each : sqlTokens) {
            result.add(each.getStartIndex() + "-" + each.getStopIndex());
        }
        return result;
    }
    
    private boolean isContainsMultiProjectionsSubstitutableToken(final SQLToken sqlToken) {
        return sqlToken instanceof SubstitutableColumnNameToken && ((SubstitutableColumnNameToken) sqlToken).getUnmodifiableProjections().size() > 1;
    }
    
    private boolean containsInScalarSubquery(final SubqueryProjectionSegment subqueryProjectionSegment, final SQLToken sqlToken) {
        return subqueryProjectionSegment.getSubquery().getSelect().getProjections().getStartIndex() <= sqlToken.getStartIndex()
                && subqueryProjectionSegment.getSubquery().getSelect().getProjections().getStopIndex() >= sqlToken.getStopIndex();
    }
    
    private Collection<ComposableSQLToken> getComposableSQLTokens(final Collection<SQLToken> previousSQLTokens) {
        Collection<ComposableSQLToken> result = new LinkedList<>();
        for (SQLToken each : previousSQLTokens) {
            if (each instanceof ComposableSQLTokenAvailable) {
                result.add(((ComposableSQLTokenAvailable) each).getComposableSQLToken());
            }
            if (each instanceof ComposableSQLToken) {
                result.add((ComposableSQLToken) each);
            }
        }
        return result;
    }
    
    private void appendRemainingSQLToken(final Collection<ComposableSQLToken> composableSQLTokens, final SQLToken remainingSQLToken, final Collection<SQLToken> sqlTokens) {
        Collection<ComposableSQLToken> containsSingleTableComposableTokens = getContainsSingleTableComposableTokens(composableSQLTokens, remainingSQLToken);
        containsSingleTableComposableTokens.forEach(sqlToken -> sqlToken.getSqlTokens().add(remainingSQLToken));
        if (containsSingleTableComposableTokens.isEmpty()) {
            sqlTokens.add(remainingSQLToken);
        }
    }
    
    private Collection<ComposableSQLToken> getContainsSingleTableComposableTokens(final Collection<ComposableSQLToken> composableSQLTokens, final SQLToken singleTableToken) {
        Collection<ComposableSQLToken> result = new LinkedList<>();
        for (ComposableSQLToken each : composableSQLTokens) {
            if (each.getStartIndex() <= singleTableToken.getStartIndex() && each.getStopIndex() >= singleTableToken.getStopIndex()) {
                result.add(each);
            }
        }
        return result;
    }
    
    @Override
    public void setSQL(final String sql) {
    }
}
