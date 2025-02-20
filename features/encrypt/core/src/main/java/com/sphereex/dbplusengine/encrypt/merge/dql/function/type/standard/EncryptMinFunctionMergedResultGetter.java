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

package com.sphereex.dbplusengine.encrypt.merge.dql.function.type.standard;

import com.sphereex.dbplusengine.encrypt.merge.dql.function.EncryptFunctionMergedResultGetter;
import org.apache.shardingsphere.encrypt.rule.EncryptRule;
import org.apache.shardingsphere.infra.database.core.type.DatabaseType;
import org.apache.shardingsphere.infra.merge.result.MergedResult;
import org.apache.shardingsphere.sql.parser.statement.core.segment.dml.expr.ExpressionSegment;
import org.apache.shardingsphere.sql.parser.statement.core.segment.dml.item.AggregationProjectionSegment;

import java.sql.SQLException;

/**
 * Encrypt max function merged result getter.
 */
public final class EncryptMinFunctionMergedResultGetter implements EncryptFunctionMergedResultGetter {
    
    private final EncryptFunctionMergedResultGetter delegated = new EncryptMaxFunctionMergedResultGetter();
    
    @Override
    public boolean isNeedDecrypt(final ExpressionSegment expressionSegment) {
        return expressionSegment instanceof AggregationProjectionSegment && getType().equalsIgnoreCase(((AggregationProjectionSegment) expressionSegment).getType().name());
    }
    
    @Override
    public Object getValue(final MergedResult mergedResult, final EncryptRule encryptRule, final DatabaseType databaseType,
                           final ExpressionSegment expressionSegment, final int columnIndex, final Class<?> type) throws SQLException {
        return delegated.getValue(mergedResult, encryptRule, databaseType, expressionSegment, columnIndex, type);
    }
    
    @Override
    public String getType() {
        return "MIN";
    }
}
