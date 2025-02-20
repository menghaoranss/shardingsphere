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

package com.sphereex.dbplusengine.encrypt.rewrite.token.job;

import com.sphereex.dbplusengine.encrypt.rewrite.aware.HintValueContextAware;
import com.sphereex.dbplusengine.encrypt.rewrite.token.job.generator.select.DecryptSelectTokenGenerator;
import com.sphereex.dbplusengine.encrypt.rewrite.token.job.generator.select.EncryptSelectTokenGenerator;
import com.sphereex.dbplusengine.encrypt.rewrite.token.job.generator.update.DecryptUpdateTokenGenerator;
import com.sphereex.dbplusengine.encrypt.rewrite.token.job.generator.update.EncryptUpdateTokenGenerator;
import com.sphereex.dbplusengine.encrypt.rewrite.token.table.generator.EncryptTableTokenGenerator;
import lombok.RequiredArgsConstructor;
import org.apache.shardingsphere.encrypt.rule.EncryptRule;
import org.apache.shardingsphere.infra.binder.context.statement.SQLStatementContext;
import org.apache.shardingsphere.infra.metadata.ShardingSphereMetaData;
import org.apache.shardingsphere.infra.metadata.database.ShardingSphereDatabase;
import org.apache.shardingsphere.infra.rewrite.context.SQLRewriteContext;
import org.apache.shardingsphere.infra.rewrite.sql.token.common.generator.SQLTokenGenerator;
import org.apache.shardingsphere.infra.rewrite.sql.token.common.generator.builder.SQLTokenGeneratorBuilder;

import java.util.Collection;
import java.util.LinkedList;

/**
 * SQL token generator builder for none unique key scenario.
 */
@RequiredArgsConstructor
public final class NoneUniqueKeyScenarioTokenGenerateBuilder implements SQLTokenGeneratorBuilder {
    
    private final SQLStatementContext sqlStatementContext;
    
    private final SQLRewriteContext sqlRewriteContext;
    
    private final EncryptRule rule;
    
    @Override
    public Collection<SQLTokenGenerator> getSQLTokenGenerators() {
        Collection<SQLTokenGenerator> result = new LinkedList<>();
        ShardingSphereMetaData metaData = sqlRewriteContext.getMetaData();
        ShardingSphereDatabase database = sqlRewriteContext.getDatabase();
        addSQLTokenGenerator(result, new EncryptSelectTokenGenerator(rule, metaData, database));
        addSQLTokenGenerator(result, new DecryptSelectTokenGenerator(rule, metaData));
        addSQLTokenGenerator(result, new EncryptUpdateTokenGenerator(rule, metaData, database));
        addSQLTokenGenerator(result, new DecryptUpdateTokenGenerator(rule, metaData));
        addSQLTokenGenerator(result, new EncryptTableTokenGenerator(rule, database));
        return result;
    }
    
    private void addSQLTokenGenerator(final Collection<SQLTokenGenerator> sqlTokenGenerators, final SQLTokenGenerator toBeAddedSQLTokenGenerator) {
        setUpSQLTokenGenerator(toBeAddedSQLTokenGenerator);
        if (toBeAddedSQLTokenGenerator.isGenerateSQLToken(sqlStatementContext)) {
            sqlTokenGenerators.add(toBeAddedSQLTokenGenerator);
        }
    }
    
    private void setUpSQLTokenGenerator(final SQLTokenGenerator toBeAddedSQLTokenGenerator) {
        if (toBeAddedSQLTokenGenerator instanceof HintValueContextAware) {
            ((HintValueContextAware) toBeAddedSQLTokenGenerator).setHintValueContext(sqlRewriteContext.getHintValueContext());
        }
    }
}
