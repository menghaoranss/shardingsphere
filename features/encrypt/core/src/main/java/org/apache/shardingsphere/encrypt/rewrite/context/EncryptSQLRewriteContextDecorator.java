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

package org.apache.shardingsphere.encrypt.rewrite.context;

import com.sphereex.dbplusengine.SphereEx;
import com.sphereex.dbplusengine.SphereEx.Type;
import com.sphereex.dbplusengine.encrypt.rewrite.token.cryptographic.util.EncryptTokenGeneratorUtils;
import com.sphereex.dbplusengine.encrypt.rewrite.token.job.NoneUniqueKeyScenarioTokenGenerateBuilder;
import com.sphereex.dbplusengine.encrypt.rewrite.util.EncryptSQLRewriteUtils;
import com.sphereex.dbplusengine.infra.hint.NoneUniqueKeyScenario;
import org.apache.shardingsphere.encrypt.constant.EncryptOrder;
import org.apache.shardingsphere.encrypt.rewrite.condition.EncryptCondition;
import org.apache.shardingsphere.encrypt.rewrite.condition.EncryptConditionEngine;
import org.apache.shardingsphere.encrypt.rewrite.parameter.EncryptParameterRewritersRegistry;
import org.apache.shardingsphere.encrypt.rewrite.token.EncryptTokenGenerateBuilder;
import org.apache.shardingsphere.encrypt.rule.EncryptRule;
import org.apache.shardingsphere.infra.annotation.HighFrequencyInvocation;
import org.apache.shardingsphere.infra.binder.context.extractor.SQLStatementContextExtractor;
import org.apache.shardingsphere.infra.binder.context.statement.SQLStatementContext;
import org.apache.shardingsphere.infra.binder.context.statement.ddl.CreateProcedureStatementContext;
import org.apache.shardingsphere.infra.binder.context.statement.dml.SelectStatementContext;
import org.apache.shardingsphere.infra.binder.context.type.TableAvailable;
import org.apache.shardingsphere.infra.binder.context.type.WhereAvailable;
import org.apache.shardingsphere.infra.config.props.ConfigurationProperties;
import org.apache.shardingsphere.infra.rewrite.context.SQLRewriteContext;
import org.apache.shardingsphere.infra.rewrite.context.SQLRewriteContextDecorator;
import org.apache.shardingsphere.infra.rewrite.parameter.rewriter.ParameterRewriter;
import org.apache.shardingsphere.infra.rewrite.parameter.rewriter.ParameterRewritersBuilder;
import org.apache.shardingsphere.infra.rewrite.sql.token.common.generator.builder.SQLTokenGeneratorBuilder;
import org.apache.shardingsphere.infra.route.context.RouteContext;
import org.apache.shardingsphere.sql.parser.statement.core.segment.dml.predicate.WhereSegment;
import org.apache.shardingsphere.sql.parser.statement.core.segment.generic.table.SimpleTableSegment;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;

/**
 * SQL rewrite context decorator for encrypt.
 */
@HighFrequencyInvocation
public final class EncryptSQLRewriteContextDecorator implements SQLRewriteContextDecorator<EncryptRule> {
    
    @Override
    public void decorate(final EncryptRule rule, final ConfigurationProperties props, final SQLRewriteContext sqlRewriteContext, final RouteContext routeContext) {
        SQLStatementContext sqlStatementContext = sqlRewriteContext.getSqlStatementContext();
        @SphereEx
        Map<String, EncryptRule> databaseEncryptRules = EncryptSQLRewriteUtils.getDatabaseEncryptRules(sqlRewriteContext.getMetaData());
        // SPEX CHANGED: BEGIN
        if (!containsEncryptTable(rule, databaseEncryptRules, sqlStatementContext) && !(sqlStatementContext instanceof CreateProcedureStatementContext)) {
            return;
        }
        Collection<EncryptCondition> encryptConditions = createEncryptConditions(rule, databaseEncryptRules, sqlStatementContext);
        // SPEX CHANGED: END
        if (!sqlRewriteContext.getParameters().isEmpty()) {
            @SphereEx(Type.MODIFY)
            EncryptParameterRewritersRegistry rewritersRegistry = new EncryptParameterRewritersRegistry(rule, sqlRewriteContext, encryptConditions, databaseEncryptRules);
            Collection<ParameterRewriter> parameterRewriters = new ParameterRewritersBuilder(sqlStatementContext).build(rewritersRegistry);
            rewriteParameters(sqlRewriteContext, parameterRewriters);
        }
        @SphereEx(Type.MODIFY)
        SQLTokenGeneratorBuilder sqlTokenGeneratorBuilder = createSQLTokenGeneratorBuilder(rule, props, sqlRewriteContext, sqlStatementContext, encryptConditions, databaseEncryptRules);
        sqlRewriteContext.addSQLTokenGenerators(sqlTokenGeneratorBuilder.getSQLTokenGenerators());
    }
    
    private boolean containsEncryptTable(final EncryptRule rule, @SphereEx final Map<String, EncryptRule> databaseEncryptRules, final SQLStatementContext sqlStatementContext) {
        if (!(sqlStatementContext instanceof TableAvailable)) {
            return false;
        }
        for (SimpleTableSegment each : ((TableAvailable) sqlStatementContext).getTablesContext().getSimpleTables()) {
            // SPEX ADDED: BEGIN
            String originalDatabaseName = each.getTableName().getTableBoundInfo().map(optional -> optional.getOriginalDatabase().getValue()).orElse("");
            EncryptRule encryptRule = databaseEncryptRules.getOrDefault(originalDatabaseName, rule);
            // SPEX ADDED: END
            // SPEX CHANGED: BEGIN
            if (encryptRule.findEncryptTable(each.getTableName().getIdentifier().getValue()).isPresent()) {
                // SPEX CHANGED: END
                return true;
            }
        }
        return false;
    }
    
    private Collection<EncryptCondition> createEncryptConditions(final EncryptRule rule, @SphereEx final Map<String, EncryptRule> databaseEncryptRules, final SQLStatementContext sqlStatementContext) {
        if (!(sqlStatementContext instanceof WhereAvailable)) {
            return Collections.emptyList();
        }
        Collection<SelectStatementContext> allSubqueryContexts = SQLStatementContextExtractor.getAllSubqueryContexts(sqlStatementContext);
        Collection<WhereSegment> whereSegments = SQLStatementContextExtractor.getWhereSegments((WhereAvailable) sqlStatementContext, allSubqueryContexts);
        // SPEX ADDED: BEGIN
        // NOTE: 加密投影列比较运算和 WHERE 条件中的比较运算改写逻辑一致，此处借用了 WHERE 中的比较运算的改写逻辑
        whereSegments.addAll(EncryptTokenGeneratorUtils.getProjectionCompareOperatorWhereSegments((WhereAvailable) sqlStatementContext, allSubqueryContexts));
        // SPEX ADDED: END
        // SPEX CHANGED: BEGIN
        return new EncryptConditionEngine(rule, databaseEncryptRules).createEncryptConditions(whereSegments);
        // SPEX CHANGED: END
    }
    
    private void rewriteParameters(final SQLRewriteContext sqlRewriteContext, final Collection<ParameterRewriter> parameterRewriters) {
        for (ParameterRewriter each : parameterRewriters) {
            each.rewrite(sqlRewriteContext.getParameterBuilder(), sqlRewriteContext.getSqlStatementContext(), sqlRewriteContext.getParameters());
        }
    }
    
    private SQLTokenGeneratorBuilder createSQLTokenGeneratorBuilder(final EncryptRule rule, @SphereEx final ConfigurationProperties props, final SQLRewriteContext sqlRewriteContext,
                                                                    final SQLStatementContext sqlStatementContext, final Collection<EncryptCondition> encryptConditions,
                                                                    @SphereEx final Map<String, EncryptRule> databaseEncryptRules) {
        // SPEX CHANGED: BEGIN
        return NoneUniqueKeyScenario.NONE == sqlRewriteContext.getHintValueContext().getNoneUniqueKeyScenario()
                ? new EncryptTokenGenerateBuilder(sqlStatementContext, encryptConditions, rule, databaseEncryptRules, sqlRewriteContext, props)
                : new NoneUniqueKeyScenarioTokenGenerateBuilder(sqlStatementContext, sqlRewriteContext, rule);
        // SPEX CHANGED: END
    }
    
    @Override
    public int getOrder() {
        return EncryptOrder.ORDER;
    }
    
    @Override
    public Class<EncryptRule> getTypeClass() {
        return EncryptRule.class;
    }
}
