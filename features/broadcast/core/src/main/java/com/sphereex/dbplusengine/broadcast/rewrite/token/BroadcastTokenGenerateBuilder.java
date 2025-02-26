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

package com.sphereex.dbplusengine.broadcast.rewrite.token;

import com.sphereex.dbplusengine.broadcast.rewrite.token.generator.BroadcastInsertValuesTokenGenerator;
import com.sphereex.dbplusengine.broadcast.rewrite.token.generator.table.BroadcastTableTokenGenerator;
import com.sphereex.dbplusengine.infra.rewrite.aware.DatabaseRuleAware;
import lombok.RequiredArgsConstructor;
import org.apache.shardingsphere.broadcast.rule.BroadcastRule;
import org.apache.shardingsphere.infra.binder.context.statement.SQLStatementContext;
import org.apache.shardingsphere.infra.config.props.ConfigurationProperties;
import org.apache.shardingsphere.infra.config.props.ConfigurationPropertyKey;
import org.apache.shardingsphere.infra.metadata.database.ShardingSphereDatabase;
import org.apache.shardingsphere.infra.rewrite.sql.token.common.generator.SQLTokenGenerator;
import org.apache.shardingsphere.infra.rewrite.sql.token.common.generator.builder.SQLTokenGeneratorBuilder;
import org.apache.shardingsphere.infra.rewrite.sql.token.keygen.generator.GeneratedKeyAssignmentTokenGenerator;
import org.apache.shardingsphere.infra.rewrite.sql.token.keygen.generator.GeneratedKeyForUseDefaultInsertColumnsTokenGenerator;
import org.apache.shardingsphere.infra.rewrite.sql.token.keygen.generator.GeneratedKeyInsertColumnTokenGenerator;
import org.apache.shardingsphere.infra.rewrite.sql.token.keygen.generator.GeneratedKeyInsertValuesTokenGenerator;

import java.util.Collection;
import java.util.LinkedList;

/**
 * SQL token generator builder for broadcast.
 */
@RequiredArgsConstructor
public final class BroadcastTokenGenerateBuilder implements SQLTokenGeneratorBuilder {
    
    private final BroadcastRule broadcastRule;
    
    private final SQLStatementContext sqlStatementContext;
    
    private final ShardingSphereDatabase database;
    
    private final ConfigurationProperties props;
    
    @Override
    public Collection<SQLTokenGenerator> getSQLTokenGenerators() {
        Collection<SQLTokenGenerator> result = new LinkedList<>();
        addSQLTokenGenerator(result, new GeneratedKeyInsertColumnTokenGenerator());
        addSQLTokenGenerator(result, new GeneratedKeyForUseDefaultInsertColumnsTokenGenerator());
        addSQLTokenGenerator(result, new GeneratedKeyAssignmentTokenGenerator());
        addSQLTokenGenerator(result, new BroadcastInsertValuesTokenGenerator());
        addSQLTokenGenerator(result, new GeneratedKeyInsertValuesTokenGenerator());
        if (props.getValue(ConfigurationPropertyKey.SCHEMA_REWRITE_ENABLED)) {
            addSQLTokenGenerator(result, new BroadcastTableTokenGenerator(database));
        }
        return result;
    }
    
    private void addSQLTokenGenerator(final Collection<SQLTokenGenerator> sqlTokenGenerators, final SQLTokenGenerator toBeAddedSQLTokenGenerator) {
        if (toBeAddedSQLTokenGenerator.isGenerateSQLToken(sqlStatementContext)) {
            sqlTokenGenerators.add(toBeAddedSQLTokenGenerator);
        }
        if (toBeAddedSQLTokenGenerator instanceof DatabaseRuleAware) {
            ((DatabaseRuleAware) toBeAddedSQLTokenGenerator).setDatabaseRule(broadcastRule);
        }
    }
}
