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

package com.sphereex.dbplusengine.single.rewrite.context;

import com.cedarsoftware.util.CaseInsensitiveMap;
import com.sphereex.dbplusengine.single.rewrite.token.SingleTokenGenerateBuilder;
import org.apache.shardingsphere.infra.annotation.HighFrequencyInvocation;
import org.apache.shardingsphere.infra.binder.context.statement.SQLStatementContext;
import org.apache.shardingsphere.infra.binder.context.type.TableAvailable;
import org.apache.shardingsphere.infra.config.props.ConfigurationProperties;
import org.apache.shardingsphere.infra.metadata.ShardingSphereMetaData;
import org.apache.shardingsphere.infra.metadata.database.ShardingSphereDatabase;
import org.apache.shardingsphere.infra.rewrite.context.SQLRewriteContext;
import org.apache.shardingsphere.infra.rewrite.context.SQLRewriteContextDecorator;
import org.apache.shardingsphere.infra.rewrite.sql.token.common.generator.SQLTokenGenerator;
import org.apache.shardingsphere.infra.route.context.RouteContext;
import org.apache.shardingsphere.infra.rule.attribute.table.TableMapperRuleAttribute;
import org.apache.shardingsphere.single.constant.SingleOrder;
import org.apache.shardingsphere.single.rule.SingleRule;
import org.apache.shardingsphere.sql.parser.statement.core.segment.generic.table.SimpleTableSegment;

import java.util.Collection;
import java.util.Map;

/**
 * SQL rewrite context decorator for single.
 */
@HighFrequencyInvocation
public final class SingleSQLRewriteContextDecorator implements SQLRewriteContextDecorator<SingleRule> {
    
    @Override
    public void decorate(final SingleRule singleRule, final ConfigurationProperties props, final SQLRewriteContext sqlRewriteContext, final RouteContext routeContext) {
        Map<String, SingleRule> databaseSingleRules = getDatabaseSingleRules(sqlRewriteContext.getMetaData());
        if (!containsSingleTable(singleRule, databaseSingleRules, sqlRewriteContext.getSqlStatementContext())) {
            return;
        }
        SQLStatementContext sqlStatementContext = sqlRewriteContext.getSqlStatementContext();
        Collection<SQLTokenGenerator> sqlTokenGenerators =
                new SingleTokenGenerateBuilder(singleRule, sqlRewriteContext.getDatabase(), sqlStatementContext, sqlRewriteContext.getMetaData()).getSQLTokenGenerators();
        sqlRewriteContext.addSQLTokenGenerators(sqlTokenGenerators);
    }
    
    private Map<String, SingleRule> getDatabaseSingleRules(final ShardingSphereMetaData metaData) {
        Map<String, SingleRule> result = new CaseInsensitiveMap<>(metaData.getAllDatabases().size(), 1F);
        for (ShardingSphereDatabase each : metaData.getAllDatabases()) {
            each.getRuleMetaData().findSingleRule(SingleRule.class).ifPresent(optional -> result.put(each.getName(), optional));
        }
        return result;
    }
    
    private boolean containsSingleTable(final SingleRule rule, final Map<String, SingleRule> databaseSingleRules, final SQLStatementContext sqlStatementContext) {
        if (!(sqlStatementContext instanceof TableAvailable)) {
            return false;
        }
        for (SimpleTableSegment each : ((TableAvailable) sqlStatementContext).getTablesContext().getSimpleTables()) {
            String originalDatabaseName = each.getTableName().getTableBoundInfo().map(optional -> optional.getOriginalDatabase().getValue()).orElse("");
            SingleRule singleRule = databaseSingleRules.getOrDefault(originalDatabaseName, rule);
            Collection<String> logicalTableNames = singleRule.getAttributes().getAttribute(TableMapperRuleAttribute.class).getLogicTableNames();
            if (logicalTableNames.contains(each.getTableName().getIdentifier().getValue())) {
                return true;
            }
        }
        return false;
    }
    
    @Override
    public int getOrder() {
        return SingleOrder.ORDER;
    }
    
    @Override
    public Class<SingleRule> getTypeClass() {
        return SingleRule.class;
    }
}
