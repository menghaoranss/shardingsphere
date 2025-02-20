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

import lombok.RequiredArgsConstructor;
import org.apache.shardingsphere.encrypt.rewrite.parameter.rewriter.EncryptInsertValueParameterRewriter;
import org.apache.shardingsphere.encrypt.rule.EncryptRule;
import org.apache.shardingsphere.infra.binder.context.statement.SQLStatementContext;
import org.apache.shardingsphere.infra.binder.context.statement.dml.InsertStatementContext;
import org.apache.shardingsphere.infra.rewrite.parameter.builder.ParameterBuilder;
import org.apache.shardingsphere.infra.rewrite.parameter.builder.impl.GroupedParameterBuilder;
import org.apache.shardingsphere.infra.rewrite.parameter.rewriter.ParameterRewriter;

import java.util.Collections;
import java.util.List;

/**
 * Multi table insert value parameter rewriter for encrypt.
 */
@RequiredArgsConstructor
public final class EncryptMultiTableInsertValueParameterRewriter implements ParameterRewriter {
    
    private final EncryptRule rule;
    
    private final String databaseName;
    
    @Override
    public boolean isNeedRewrite(final SQLStatementContext sqlStatementContext) {
        return sqlStatementContext instanceof InsertStatementContext && !((InsertStatementContext) sqlStatementContext).getMultiInsertStatementContexts().isEmpty();
    }
    
    @Override
    public void rewrite(final ParameterBuilder paramBuilder, final SQLStatementContext sqlStatementContext, final List<Object> params) {
        InsertStatementContext insertStatementContext = (InsertStatementContext) sqlStatementContext;
        EncryptInsertValueParameterRewriter parameterRewriter = new EncryptInsertValueParameterRewriter(rule, databaseName);
        int parameterOffset = 0;
        int parameterGroupOffset = 0;
        for (InsertStatementContext each : insertStatementContext.getMultiInsertStatementContexts()) {
            if (parameterRewriter.isNeedRewrite(each) && paramBuilder instanceof GroupedParameterBuilder) {
                List<Object> subInsertParams = params.subList(parameterOffset, each.getSqlStatement().getParameterCount());
                parameterOffset += each.getSqlStatement().getParameterCount();
                GroupedParameterBuilder groupedParameterBuilder = new GroupedParameterBuilder(each.getGroupedParameters(), Collections.emptyList());
                parameterRewriter.rewrite(groupedParameterBuilder, each, subInsertParams);
                for (int index = 0; index < groupedParameterBuilder.getParameterBuilders().size(); index++) {
                    ((GroupedParameterBuilder) paramBuilder).getParameterBuilders().set(parameterGroupOffset + index, groupedParameterBuilder.getParameterBuilders().get(index));
                    parameterGroupOffset += 1;
                }
            }
        }
    }
}
