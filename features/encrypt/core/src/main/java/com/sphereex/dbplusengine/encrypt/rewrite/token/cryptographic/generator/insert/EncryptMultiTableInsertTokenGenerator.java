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
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import org.apache.shardingsphere.encrypt.rewrite.aware.EncryptConditionsAware;
import org.apache.shardingsphere.encrypt.rewrite.condition.EncryptCondition;
import org.apache.shardingsphere.encrypt.rewrite.token.EncryptTokenGenerateBuilder;
import org.apache.shardingsphere.encrypt.rule.EncryptRule;
import org.apache.shardingsphere.infra.binder.context.statement.SQLStatementContext;
import org.apache.shardingsphere.infra.binder.context.statement.dml.InsertStatementContext;
import org.apache.shardingsphere.infra.config.props.ConfigurationProperties;
import org.apache.shardingsphere.infra.rewrite.context.SQLRewriteContext;
import org.apache.shardingsphere.infra.rewrite.sql.token.common.generator.CollectionSQLTokenGenerator;
import org.apache.shardingsphere.infra.rewrite.sql.token.common.generator.SQLTokenGenerators;
import org.apache.shardingsphere.infra.rewrite.sql.token.common.generator.aware.ConnectionContextAware;
import org.apache.shardingsphere.infra.rewrite.sql.token.common.pojo.SQLToken;
import org.apache.shardingsphere.infra.session.connection.ConnectionContext;

import java.util.Collection;
import java.util.LinkedList;
import java.util.Map;

/**
 * Multi table insert token generator for encrypt.
 */
@RequiredArgsConstructor
@Setter
public final class EncryptMultiTableInsertTokenGenerator
        implements
            CollectionSQLTokenGenerator<InsertStatementContext>,
            EncryptConditionsAware,
            ConnectionContextAware,
            ConfigurationPropertiesAware {
    
    private final EncryptRule rule;
    
    private final Map<String, EncryptRule> databaseEncryptRules;
    
    private final SQLRewriteContext sqlRewriteContext;
    
    private Collection<EncryptCondition> encryptConditions;
    
    private ConnectionContext connectionContext;
    
    private ConfigurationProperties props;
    
    @Override
    public boolean isGenerateSQLToken(final SQLStatementContext sqlStatementContext) {
        return sqlStatementContext instanceof InsertStatementContext && !((InsertStatementContext) sqlStatementContext).getMultiInsertStatementContexts().isEmpty();
    }
    
    @Override
    public Collection<SQLToken> generateSQLTokens(final InsertStatementContext insertStatementContext) {
        Collection<SQLToken> result = new LinkedList<>();
        for (InsertStatementContext each : insertStatementContext.getMultiInsertStatementContexts()) {
            SQLTokenGenerators sqlTokenGenerators = new SQLTokenGenerators();
            sqlTokenGenerators.addAll(new EncryptTokenGenerateBuilder(each, encryptConditions, rule, databaseEncryptRules, sqlRewriteContext, props).getSQLTokenGenerators());
            result.addAll(sqlTokenGenerators.generateSQLTokens(sqlRewriteContext.getDatabase(), each, sqlRewriteContext.getParameters(), connectionContext));
        }
        return result;
    }
}
