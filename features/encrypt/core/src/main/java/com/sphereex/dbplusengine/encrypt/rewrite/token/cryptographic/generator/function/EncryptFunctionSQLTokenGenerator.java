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

package com.sphereex.dbplusengine.encrypt.rewrite.token.cryptographic.generator.function;

import org.apache.shardingsphere.encrypt.rule.EncryptRule;
import org.apache.shardingsphere.infra.metadata.ShardingSphereMetaData;
import org.apache.shardingsphere.infra.metadata.database.ShardingSphereDatabase;
import org.apache.shardingsphere.infra.rewrite.sql.token.common.pojo.SQLToken;
import org.apache.shardingsphere.infra.spi.annotation.SingletonSPI;
import org.apache.shardingsphere.infra.spi.type.typed.TypedSPI;
import org.apache.shardingsphere.sql.parser.statement.core.segment.dml.expr.ExpressionSegment;

import java.util.Collection;
import java.util.Map;

/**
 * Encrypt function SQL token generator.
 */
@SingletonSPI
public interface EncryptFunctionSQLTokenGenerator extends TypedSPI {
    
    /**
     * Judge is need generate SQL token or not.
     *
     * @param expressionSegment expression segment
     * @return is need generate SQL token or not
     */
    boolean isGenerateSQLToken(ExpressionSegment expressionSegment);
    
    /**
     * Generate SQL tokens.
     *
     * @param encryptRule encrypt rule
     * @param databaseEncryptRules database encrypt rules
     * @param database database
     * @param metaData meta data
     * @param expressionSegment expression segment
     * @return generated SQL tokens
     */
    Collection<SQLToken> generateSQLTokens(EncryptRule encryptRule, Map<String, EncryptRule> databaseEncryptRules,
                                           ShardingSphereDatabase database, ShardingSphereMetaData metaData, ExpressionSegment expressionSegment);
}
