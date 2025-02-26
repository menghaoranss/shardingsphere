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

package com.sphereex.dbplusengine.broadcast.rewrite.context;

import com.sphereex.dbplusengine.broadcast.rewrite.parameter.BroadcastParameterRewriterRegistry;
import com.sphereex.dbplusengine.broadcast.rewrite.token.BroadcastTokenGenerateBuilder;
import org.apache.shardingsphere.broadcast.constant.BroadcastOrder;
import org.apache.shardingsphere.broadcast.rule.BroadcastRule;
import org.apache.shardingsphere.infra.annotation.HighFrequencyInvocation;
import org.apache.shardingsphere.infra.binder.context.statement.SQLStatementContext;
import org.apache.shardingsphere.infra.binder.context.statement.dml.InsertStatementContext;
import org.apache.shardingsphere.infra.binder.context.type.TableAvailable;
import org.apache.shardingsphere.infra.config.props.ConfigurationProperties;
import org.apache.shardingsphere.infra.rewrite.context.SQLRewriteContext;
import org.apache.shardingsphere.infra.rewrite.context.SQLRewriteContextDecorator;
import org.apache.shardingsphere.infra.rewrite.parameter.rewriter.ParameterRewriter;
import org.apache.shardingsphere.infra.rewrite.parameter.rewriter.ParameterRewritersBuilder;
import org.apache.shardingsphere.infra.route.context.RouteContext;

import java.util.Collection;

/**
 * SQL rewrite context decorator for broadcast.
 */
@HighFrequencyInvocation
public final class BroadcastSQLRewriteContextDecorator implements SQLRewriteContextDecorator<BroadcastRule> {
    
    @Override
    public void decorate(final BroadcastRule broadcastRule, final ConfigurationProperties props, final SQLRewriteContext sqlRewriteContext, final RouteContext routeContext) {
        SQLStatementContext sqlStatementContext = sqlRewriteContext.getSqlStatementContext();
        if (!isNeedDecorate(broadcastRule, sqlStatementContext)) {
            return;
        }
        if (!sqlRewriteContext.getParameters().isEmpty()) {
            Collection<ParameterRewriter> parameterRewriters = new ParameterRewritersBuilder(sqlStatementContext).build(new BroadcastParameterRewriterRegistry());
            rewriteParameters(sqlRewriteContext, parameterRewriters);
        }
        // SPEX CHANGED: BEGIN
        sqlRewriteContext
                .addSQLTokenGenerators(new BroadcastTokenGenerateBuilder(broadcastRule, sqlStatementContext, sqlRewriteContext.getDatabase(), props).getSQLTokenGenerators());
        // SPEX CHANGED: END
    }
    
    private boolean isNeedDecorate(final BroadcastRule broadcastRule, final SQLStatementContext sqlStatementContext) {
        if (sqlStatementContext instanceof TableAvailable && containsBroadcastTable(broadcastRule, (TableAvailable) sqlStatementContext)) {
            return true;
        }
        return sqlStatementContext instanceof InsertStatementContext && containsBroadcastTableKeyGenerateStrategy(broadcastRule, (InsertStatementContext) sqlStatementContext);
    }
    
    private boolean containsBroadcastTable(final BroadcastRule broadcastRule, final TableAvailable sqlStatementContext) {
        if (sqlStatementContext.getTablesContext().getTableNames().isEmpty()) {
            return false;
        }
        for (String each : sqlStatementContext.getTablesContext().getTableNames()) {
            if (broadcastRule.getTables().contains(each)) {
                return true;
            }
        }
        return false;
    }
    
    private boolean containsBroadcastTableKeyGenerateStrategy(final BroadcastRule broadcastRule, final InsertStatementContext sqlStatementContext) {
        for (String each : sqlStatementContext.getTablesContext().getTableNames()) {
            if (broadcastRule.getTables().contains(each) && broadcastRule.findKeyGenerateStrategyByTable(each).isPresent()) {
                return true;
            }
        }
        return false;
    }
    
    private void rewriteParameters(final SQLRewriteContext sqlRewriteContext, final Collection<ParameterRewriter> parameterRewriters) {
        for (ParameterRewriter each : parameterRewriters) {
            each.rewrite(sqlRewriteContext.getParameterBuilder(), sqlRewriteContext.getSqlStatementContext(), sqlRewriteContext.getParameters());
        }
    }
    
    @Override
    public int getOrder() {
        return BroadcastOrder.ORDER;
    }
    
    @Override
    public Class<BroadcastRule> getTypeClass() {
        return BroadcastRule.class;
    }
}
