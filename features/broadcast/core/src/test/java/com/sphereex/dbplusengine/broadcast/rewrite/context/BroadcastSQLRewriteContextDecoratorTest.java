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

import org.apache.shardingsphere.broadcast.rule.BroadcastRule;
import org.apache.shardingsphere.infra.binder.context.statement.SQLStatementContext;
import org.apache.shardingsphere.infra.binder.context.statement.dml.InsertStatementContext;
import org.apache.shardingsphere.infra.binder.context.statement.dml.SelectStatementContext;
import org.apache.shardingsphere.infra.config.props.ConfigurationProperties;
import org.apache.shardingsphere.infra.config.props.ConfigurationPropertyKey;
import org.apache.shardingsphere.infra.rewrite.context.SQLRewriteContext;
import org.apache.shardingsphere.infra.route.context.RouteContext;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class BroadcastSQLRewriteContextDecoratorTest {
    
    private final BroadcastSQLRewriteContextDecorator decorator = new BroadcastSQLRewriteContextDecorator();
    
    @Test
    void assertDecorateWithSelectStatementContext() {
        SQLRewriteContext sqlRewriteContext = mock(SQLRewriteContext.class);
        SQLStatementContext sqlStatementContext = mock(SelectStatementContext.class, RETURNS_DEEP_STUBS);
        when(sqlRewriteContext.getSqlStatementContext()).thenReturn(sqlStatementContext);
        decorator.decorate(mock(BroadcastRule.class), mock(ConfigurationProperties.class), sqlRewriteContext, mock(RouteContext.class));
        assertTrue(sqlRewriteContext.getSqlTokens().isEmpty());
    }
    
    @Test
    void assertDecorateWhenNotContainsBroadcastTableKeyGenerateStrategy() {
        SQLRewriteContext sqlRewriteContext = mock(SQLRewriteContext.class);
        InsertStatementContext sqlStatementContext = mock(InsertStatementContext.class, RETURNS_DEEP_STUBS);
        when(sqlStatementContext.getTablesContext().getTableNames()).thenReturn(Collections.singleton("t_order"));
        when(sqlRewriteContext.getSqlStatementContext()).thenReturn(sqlStatementContext);
        BroadcastRule broadcastRule = mock(BroadcastRule.class, RETURNS_DEEP_STUBS);
        when(broadcastRule.getTables().contains("t_order")).thenReturn(true);
        when(broadcastRule.findKeyGenerateStrategyByTable("t_order").isPresent()).thenReturn(true);
        decorator.decorate(broadcastRule, mockConfigurationProperties(), sqlRewriteContext, mock(RouteContext.class));
        assertTrue(sqlRewriteContext.getSqlTokens().isEmpty());
    }
    
    private ConfigurationProperties mockConfigurationProperties() {
        ConfigurationProperties result = mock(ConfigurationProperties.class, RETURNS_DEEP_STUBS);
        when(result.getValue(ConfigurationPropertyKey.SCHEMA_REWRITE_ENABLED)).thenReturn(false);
        return result;
    }
}
