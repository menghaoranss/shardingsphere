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

package com.sphereex.dbplusengine.single.rewrite.token;

import com.sphereex.dbplusengine.infra.rewrite.aware.DatabaseRuleAware;
import com.sphereex.dbplusengine.infra.rewrite.aware.GlobalMetaDataAware;
import com.sphereex.dbplusengine.single.rewrite.aware.SingleRuleAware;
import com.sphereex.dbplusengine.single.rewrite.token.generator.SingleTableTokenGenerator;
import lombok.RequiredArgsConstructor;
import org.apache.shardingsphere.infra.binder.context.statement.SQLStatementContext;
import org.apache.shardingsphere.infra.metadata.ShardingSphereMetaData;
import org.apache.shardingsphere.infra.metadata.database.ShardingSphereDatabase;
import org.apache.shardingsphere.infra.rewrite.sql.token.common.generator.SQLTokenGenerator;
import org.apache.shardingsphere.infra.rewrite.sql.token.common.generator.builder.SQLTokenGeneratorBuilder;
import org.apache.shardingsphere.single.rule.SingleRule;

import java.util.Collection;
import java.util.LinkedList;

/**
 * SQL token generator builder for single.
 */
@RequiredArgsConstructor
public final class SingleTokenGenerateBuilder implements SQLTokenGeneratorBuilder {
    
    private final SingleRule singleRule;
    
    private final ShardingSphereDatabase database;
    
    private final SQLStatementContext sqlStatementContext;
    
    private final ShardingSphereMetaData metaData;
    
    @Override
    public Collection<SQLTokenGenerator> getSQLTokenGenerators() {
        Collection<SQLTokenGenerator> result = new LinkedList<>();
        addSQLTokenGenerator(result, new SingleTableTokenGenerator());
        return result;
    }
    
    private void addSQLTokenGenerator(final Collection<SQLTokenGenerator> sqlTokenGenerators, final SQLTokenGenerator toBeAddedSQLTokenGenerator) {
        if (toBeAddedSQLTokenGenerator.isGenerateSQLToken(sqlStatementContext)) {
            setUpSQLTokenGenerator(toBeAddedSQLTokenGenerator);
            sqlTokenGenerators.add(toBeAddedSQLTokenGenerator);
        }
    }
    
    private void setUpSQLTokenGenerator(final SQLTokenGenerator toBeAddedSQLTokenGenerator) {
        if (toBeAddedSQLTokenGenerator instanceof SingleRuleAware) {
            ((SingleRuleAware) toBeAddedSQLTokenGenerator).setSingleRule(singleRule);
        }
        if (toBeAddedSQLTokenGenerator instanceof DatabaseRuleAware) {
            ((DatabaseRuleAware) toBeAddedSQLTokenGenerator).setDatabaseRule(singleRule);
        }
        if (toBeAddedSQLTokenGenerator instanceof GlobalMetaDataAware) {
            ((GlobalMetaDataAware) toBeAddedSQLTokenGenerator).setMetaData(metaData);
        }
    }
}
