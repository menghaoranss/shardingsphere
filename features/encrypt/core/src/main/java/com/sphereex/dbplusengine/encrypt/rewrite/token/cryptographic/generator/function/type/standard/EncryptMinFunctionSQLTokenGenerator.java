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

package com.sphereex.dbplusengine.encrypt.rewrite.token.cryptographic.generator.function.type.standard;

import com.sphereex.dbplusengine.encrypt.rewrite.token.cryptographic.generator.function.EncryptFunctionSQLTokenGenerator;
import org.apache.shardingsphere.encrypt.rule.EncryptRule;
import org.apache.shardingsphere.infra.annotation.HighFrequencyInvocation;
import org.apache.shardingsphere.infra.metadata.ShardingSphereMetaData;
import org.apache.shardingsphere.infra.metadata.database.ShardingSphereDatabase;
import org.apache.shardingsphere.infra.rewrite.sql.token.common.pojo.SQLToken;
import org.apache.shardingsphere.sql.parser.statement.core.segment.dml.expr.ExpressionSegment;
import org.apache.shardingsphere.sql.parser.statement.core.segment.dml.item.AggregationProjectionSegment;

import java.util.Collection;
import java.util.Map;

/**
 * Encrypt min function encrypt SQL token generator.
 */
@HighFrequencyInvocation
public final class EncryptMinFunctionSQLTokenGenerator implements EncryptFunctionSQLTokenGenerator {
    
    private final EncryptFunctionSQLTokenGenerator delegated = new EncryptMaxFunctionSQLTokenGenerator();
    
    @Override
    public boolean isGenerateSQLToken(final ExpressionSegment expressionSegment) {
        return expressionSegment instanceof AggregationProjectionSegment && getType().equalsIgnoreCase(((AggregationProjectionSegment) expressionSegment).getType().name());
    }
    
    @Override
    public Collection<SQLToken> generateSQLTokens(final EncryptRule encryptRule, final Map<String, EncryptRule> databaseEncryptRules, final ShardingSphereDatabase database,
                                                  final ShardingSphereMetaData metaData, final ExpressionSegment expressionSegment) {
        return delegated.generateSQLTokens(encryptRule, databaseEncryptRules, database, metaData, expressionSegment);
    }
    
    @Override
    public String getType() {
        return "MIN";
    }
}
