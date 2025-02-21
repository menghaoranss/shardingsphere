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

package com.sphereex.dbplusengine.encrypt.rewrite.token.cryptographic.generator.select;

import com.sphereex.dbplusengine.encrypt.checker.cryptographic.UsingConditionsEncryptorChecker;
import com.sphereex.dbplusengine.encrypt.rewrite.token.cryptographic.pojo.EncryptJoinUsingColumnsToken;
import com.sphereex.dbplusengine.encrypt.rewrite.token.cryptographic.pojo.EncryptNaturalJoinToken;
import com.sphereex.dbplusengine.encrypt.rewrite.token.cryptographic.util.EncryptTokenGeneratorUtils;
import lombok.RequiredArgsConstructor;
import org.apache.shardingsphere.encrypt.rule.EncryptRule;
import org.apache.shardingsphere.infra.binder.context.segment.table.TablesContext;
import org.apache.shardingsphere.infra.binder.context.statement.SQLStatementContext;
import org.apache.shardingsphere.infra.binder.context.statement.dml.SelectStatementContext;
import org.apache.shardingsphere.infra.rewrite.sql.token.common.generator.CollectionSQLTokenGenerator;
import org.apache.shardingsphere.infra.rewrite.sql.token.common.pojo.SQLToken;
import org.apache.shardingsphere.sql.parser.statement.core.segment.generic.table.JoinTableSegment;
import org.apache.shardingsphere.sql.parser.statement.core.segment.generic.table.TableSegment;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Map;

/**
 * Using natural join token generator for encrypt.
 */
@RequiredArgsConstructor
public final class EncryptUsingNaturalJoinTokenGenerator implements CollectionSQLTokenGenerator<SelectStatementContext> {
    
    private final EncryptRule rule;
    
    private final Map<String, EncryptRule> databaseEncryptRules;
    
    @Override
    public boolean isGenerateSQLToken(final SQLStatementContext sqlStatementContext) {
        return sqlStatementContext instanceof SelectStatementContext && ((SelectStatementContext) sqlStatementContext).isContainsJoinQuery();
    }
    
    @Override
    public Collection<SQLToken> generateSQLTokens(final SelectStatementContext sqlStatementContext) {
        TableSegment tableSegment = sqlStatementContext.getSqlStatement().getFrom().orElse(null);
        return EncryptTokenGeneratorUtils.isNeedRewriteUsingNaturalJoin(sqlStatementContext, rule, databaseEncryptRules)
                ? generateSQLTokens(tableSegment, sqlStatementContext.getTablesContext())
                : Collections.emptyList();
    }
    
    private Collection<SQLToken> generateSQLTokens(final TableSegment tableSegment, final TablesContext tablesContext) {
        if (!(tableSegment instanceof JoinTableSegment)) {
            return Collections.emptyList();
        }
        Collection<SQLToken> result = new LinkedHashSet<>();
        JoinTableSegment joinTableSegment = (JoinTableSegment) tableSegment;
        if (!joinTableSegment.getUsing().isEmpty()) {
            UsingConditionsEncryptorChecker.checkIsSame(joinTableSegment.getUsing(), rule, databaseEncryptRules, "using condition");
            result.add(new EncryptJoinUsingColumnsToken(joinTableSegment.getRight().getStopIndex() + 1, tableSegment.getStopIndex(), joinTableSegment.getUsing(), tablesContext, rule,
                    databaseEncryptRules));
            result.addAll(generateSQLTokens(joinTableSegment.getLeft(), tablesContext));
            result.addAll(generateSQLTokens(joinTableSegment.getRight(), tablesContext));
        }
        if (joinTableSegment.isNatural()) {
            UsingConditionsEncryptorChecker.checkIsSame(joinTableSegment.getDerivedUsing(), rule, databaseEncryptRules, "natural join condition");
            result.add(new EncryptJoinUsingColumnsToken(joinTableSegment.getRight().getStopIndex() + 1, tableSegment.getStopIndex(), joinTableSegment.getDerivedUsing(), tablesContext, rule,
                    databaseEncryptRules));
            result.add(new EncryptNaturalJoinToken(joinTableSegment.getLeft().getStopIndex() + 1, joinTableSegment.getRight().getStartIndex() - 1, joinTableSegment.getJoinType()));
            result.addAll(generateSQLTokens(joinTableSegment.getLeft(), tablesContext));
            result.addAll(generateSQLTokens(joinTableSegment.getRight(), tablesContext));
        }
        return result;
    }
}
