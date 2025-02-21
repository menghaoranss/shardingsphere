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

package com.sphereex.dbplusengine.encrypt.rewrite.token.table.generator;

import com.cedarsoftware.util.CaseInsensitiveSet;
import com.sphereex.dbplusengine.encrypt.config.rule.mode.EncryptModeType;
import com.sphereex.dbplusengine.encrypt.rewrite.token.table.pojo.EncryptTableToken;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import org.apache.shardingsphere.encrypt.rule.EncryptRule;
import org.apache.shardingsphere.infra.binder.context.aware.CursorAware;
import org.apache.shardingsphere.infra.binder.context.statement.SQLStatementContext;
import org.apache.shardingsphere.infra.binder.context.type.TableAvailable;
import org.apache.shardingsphere.infra.metadata.database.ShardingSphereDatabase;
import org.apache.shardingsphere.infra.rewrite.sql.token.common.generator.CollectionSQLTokenGenerator;
import org.apache.shardingsphere.infra.rewrite.sql.token.common.generator.aware.PreviousSQLTokensAware;
import org.apache.shardingsphere.infra.rewrite.sql.token.common.pojo.SQLToken;
import org.apache.shardingsphere.sql.parser.statement.core.segment.generic.table.SimpleTableSegment;
import org.apache.shardingsphere.sql.parser.statement.core.segment.generic.table.TableNameSegment;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

/**
 * Encrypt table token generator.
 */
@RequiredArgsConstructor
@Setter
public final class EncryptTableTokenGenerator implements CollectionSQLTokenGenerator<SQLStatementContext>, PreviousSQLTokensAware {
    
    private final EncryptRule rule;
    
    private final ShardingSphereDatabase database;
    
    private List<SQLToken> previousSQLTokens;
    
    @Override
    public boolean isGenerateSQLToken(final SQLStatementContext sqlStatementContext) {
        if (!(sqlStatementContext instanceof TableAvailable) || sqlStatementContext instanceof CursorAware) {
            return false;
        }
        return EncryptModeType.FRONTEND != rule.getEncryptMode().getType();
    }
    
    @Override
    public Collection<SQLToken> generateSQLTokens(final SQLStatementContext sqlStatementContext) {
        Collection<SQLToken> result = new LinkedList<>();
        Collection<Integer> previousSQLTokenStartIndexes = buildPreviousSQLTokenStartIndexes(previousSQLTokens);
        for (SimpleTableSegment each : ((TableAvailable) sqlStatementContext).getTablesContext().getSimpleTables()) {
            TableNameSegment tableName = each.getTableName();
            int startIndex = tableName.getStartIndex();
            if (!rule.findEncryptTable(tableName.getIdentifier().getValue()).isPresent() || previousSQLTokenStartIndexes.contains(startIndex)) {
                continue;
            }
            result.add(new EncryptTableToken(startIndex, tableName.getStopIndex(), tableName.getIdentifier(), database));
        }
        return result;
    }
    
    private Collection<Integer> buildPreviousSQLTokenStartIndexes(final List<SQLToken> previousSQLTokens) {
        Collection<Integer> result = new CaseInsensitiveSet<>();
        for (SQLToken each : previousSQLTokens) {
            result.add(each.getStartIndex());
        }
        return result;
    }
}
