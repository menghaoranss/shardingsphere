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

package org.apache.shardingsphere.encrypt.checker.sql.projection;

import com.sphereex.dbplusengine.SphereEx;
import com.sphereex.dbplusengine.SphereEx.Type;
import com.sphereex.dbplusengine.encrypt.rewrite.token.comparator.CombineProjectionColumnsEncryptorComparator;
import com.sphereex.dbplusengine.encrypt.rewrite.util.EncryptSQLRewriteUtils;
import org.apache.shardingsphere.encrypt.rule.EncryptRule;
import org.apache.shardingsphere.infra.annotation.HighFrequencyInvocation;
import org.apache.shardingsphere.infra.binder.context.statement.SQLStatementContext;
import org.apache.shardingsphere.infra.binder.context.statement.dml.SelectStatementContext;
import org.apache.shardingsphere.infra.checker.SupportedSQLChecker;
import org.apache.shardingsphere.infra.exception.core.ShardingSpherePreconditions;
import org.apache.shardingsphere.infra.exception.generic.UnsupportedSQLOperationException;
import org.apache.shardingsphere.infra.metadata.ShardingSphereMetaData;
import org.apache.shardingsphere.infra.metadata.database.ShardingSphereDatabase;
import org.apache.shardingsphere.infra.metadata.database.schema.model.ShardingSphereSchema;

import java.util.Map;

/**
 * Select projection supported checker for encrypt.
 */
@HighFrequencyInvocation
public final class EncryptSelectProjectionSupportedChecker implements SupportedSQLChecker<SelectStatementContext, EncryptRule> {
    
    @Override
    public boolean isCheck(final SQLStatementContext sqlStatementContext) {
        return sqlStatementContext instanceof SelectStatementContext && !((SelectStatementContext) sqlStatementContext).getTablesContext().getSimpleTables().isEmpty();
    }
    
    @Override
    public void check(final EncryptRule rule, @SphereEx final ShardingSphereMetaData metaData, final ShardingSphereDatabase database, final ShardingSphereSchema currentSchema,
                      final SelectStatementContext sqlStatementContext) {
        @SphereEx
        Map<String, EncryptRule> databaseEncryptRules = EncryptSQLRewriteUtils.getDatabaseEncryptRules(metaData);
        // SPEX CHANGED: BEGIN
        checkSelectStatementContext(rule, sqlStatementContext, databaseEncryptRules);
        // SPEX CHANGED: END
        for (SelectStatementContext each : sqlStatementContext.getSubqueryContexts().values()) {
            // SPEX CHANGED: BEGIN
            checkSelectStatementContext(rule, each, databaseEncryptRules);
            // SPEX CHANGED: END
        }
    }
    
    @SphereEx(Type.MODIFY)
    private void checkSelectStatementContext(final EncryptRule rule, final SelectStatementContext selectStatementContext, @SphereEx final Map<String, EncryptRule> databaseEncryptRules) {
        ShardingSpherePreconditions.checkState(!selectStatementContext.isContainsCombine() || CombineProjectionColumnsEncryptorComparator.isSame(selectStatementContext, rule, databaseEncryptRules),
                () -> new UnsupportedSQLOperationException("Can not use different encryptor for projection columns in combine statement"));
    }
}
