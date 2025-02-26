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

package com.sphereex.dbplusengine.broadcast.route.engine.keygen;

import com.sphereex.dbplusengine.broadcast.config.keygen.BroadcastKeyGenerateStrategyConfiguration;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.apache.shardingsphere.broadcast.rule.BroadcastRule;
import org.apache.shardingsphere.infra.algorithm.core.context.AlgorithmSQLContext;
import org.apache.shardingsphere.infra.binder.context.segment.insert.keygen.GeneratedKeyContext;
import org.apache.shardingsphere.infra.binder.context.statement.SQLStatementContext;
import org.apache.shardingsphere.infra.binder.context.statement.dml.InsertStatementContext;
import org.apache.shardingsphere.infra.database.core.type.DatabaseTypeRegistry;

import java.util.Optional;

/**
 * Broadcast key generate engine.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class BroadcastKeyGenerateEngine {
    
    /**
     * Generate keys.
     *
     * @param sqlStatementContext SQL statement context
     * @param rule broadcast rule
     * @param databaseName database name
     */
    public static void generateKeys(final SQLStatementContext sqlStatementContext, final BroadcastRule rule, final String databaseName) {
        if (!(sqlStatementContext instanceof InsertStatementContext)) {
            return;
        }
        InsertStatementContext insertStatementContext = (InsertStatementContext) sqlStatementContext;
        Optional<GeneratedKeyContext> generatedKey = insertStatementContext.getGeneratedKeyContext();
        String tableName = insertStatementContext.getSqlStatement().getTable().map(optional -> optional.getTableName().getIdentifier().getValue()).orElse("");
        Optional<BroadcastKeyGenerateStrategyConfiguration> keyGenerateStrategy = rule.findKeyGenerateStrategyByTable(tableName);
        if (generatedKey.isPresent() && generatedKey.get().isGenerated() && keyGenerateStrategy.isPresent()) {
            String schemaName = insertStatementContext.getTablesContext().getSchemaName()
                    .orElseGet(() -> new DatabaseTypeRegistry(sqlStatementContext.getDatabaseType()).getDefaultSchemaName(databaseName));
            AlgorithmSQLContext algorithmSQLContext = new AlgorithmSQLContext(databaseName, schemaName, tableName, keyGenerateStrategy.get().getKeyGenerateColumn());
            generatedKey.get().getGeneratedValues().addAll(rule.generateKeys(algorithmSQLContext, insertStatementContext.getValueListCount()));
            generatedKey.get().setSupportAutoIncrement(rule.isSupportAutoIncrement(databaseName, tableName));
        }
    }
}
