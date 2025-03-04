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

package com.sphereex.dbplusengine.infra.rewrite.parameter.rewriter.keygen;

import org.apache.shardingsphere.infra.binder.context.statement.SQLStatementContext;
import org.apache.shardingsphere.infra.binder.context.statement.dml.InsertStatementContext;
import org.apache.shardingsphere.infra.rewrite.parameter.builder.ParameterBuilder;
import org.apache.shardingsphere.infra.rewrite.parameter.builder.impl.GroupedParameterBuilder;
import org.apache.shardingsphere.infra.rewrite.parameter.rewriter.ParameterRewriter;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * Generated key multi insert value parameter rewriter.
 */
public final class GeneratedKeyMultiInsertValueParameterRewriter implements ParameterRewriter {
    
    @Override
    public boolean isNeedRewrite(final SQLStatementContext sqlStatementContext) {
        return sqlStatementContext instanceof InsertStatementContext && !((InsertStatementContext) sqlStatementContext).getMultiInsertStatementContexts().isEmpty();
    }
    
    @Override
    public void rewrite(final ParameterBuilder paramBuilder, final SQLStatementContext sqlStatementContext, final List<Object> params) {
        InsertStatementContext insertStatementContext = (InsertStatementContext) sqlStatementContext;
        int index = 0;
        int parameterCount = 0;
        for (InsertStatementContext each : insertStatementContext.getMultiInsertStatementContexts()) {
            if (!each.getGeneratedKeyContext().isPresent()) {
                continue;
            }
            GroupedParameterBuilder parameterBuilder = (GroupedParameterBuilder) paramBuilder;
            Iterator<Comparable<?>> generatedValues = each.getGeneratedKeyContext().get().getGeneratedValues().iterator();
            parameterCount += each.getInsertValueContexts().iterator().next().getParameterCount();
            parameterBuilder.setDerivedColumnName(each.getGeneratedKeyContext().get().getColumnName());
            parameterBuilder.getParameterBuilders().get(index++).addAddedParameters(parameterCount, new ArrayList<>(Collections.singleton(generatedValues.next())));
        }
    }
}
