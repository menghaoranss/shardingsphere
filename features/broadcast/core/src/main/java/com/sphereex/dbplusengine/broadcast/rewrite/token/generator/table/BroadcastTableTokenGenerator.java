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

package com.sphereex.dbplusengine.broadcast.rewrite.token.generator.table;

import com.sphereex.dbplusengine.broadcast.rewrite.token.pojo.BroadcastTableToken;
import com.sphereex.dbplusengine.infra.rewrite.token.TableTokenUtils;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import org.apache.shardingsphere.broadcast.rule.BroadcastRule;
import org.apache.shardingsphere.infra.binder.context.aware.CursorAware;
import org.apache.shardingsphere.infra.binder.context.statement.SQLStatementContext;
import org.apache.shardingsphere.infra.binder.context.type.TableAvailable;
import org.apache.shardingsphere.infra.metadata.database.ShardingSphereDatabase;
import org.apache.shardingsphere.infra.rewrite.sql.token.common.generator.CollectionSQLTokenGenerator;
import org.apache.shardingsphere.infra.rewrite.sql.token.common.pojo.SQLToken;
import org.apache.shardingsphere.infra.rule.attribute.table.TableMapperRuleAttribute;
import org.apache.shardingsphere.sql.parser.statement.core.segment.generic.table.SimpleTableSegment;
import org.apache.shardingsphere.sql.parser.statement.core.segment.generic.table.TableNameSegment;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.Optional;

/**
 * Broadcast table token generator.
 */
@RequiredArgsConstructor
@Setter
public final class BroadcastTableTokenGenerator implements CollectionSQLTokenGenerator<SQLStatementContext> {
    
    private final ShardingSphereDatabase database;
    
    private Collection<SimpleTableSegment> allTables;
    
    @Override
    public boolean isGenerateSQLToken(final SQLStatementContext sqlStatementContext) {
        if (!(sqlStatementContext instanceof TableAvailable) || sqlStatementContext instanceof CursorAware) {
            return false;
        }
        allTables = ((TableAvailable) sqlStatementContext).getTablesContext().getSimpleTables();
        return !allTables.isEmpty();
    }
    
    @Override
    public Collection<SQLToken> generateSQLTokens(final SQLStatementContext sqlStatementContext) {
        if (!(sqlStatementContext instanceof TableAvailable) || !TableTokenUtils.isNeedSchemaRewrite(database.getResourceMetaData().getStorageUnits())) {
            return Collections.emptyList();
        }
        Optional<BroadcastRule> broadcastRule = database.getRuleMetaData().findSingleRule(BroadcastRule.class);
        if (!broadcastRule.isPresent()) {
            return Collections.emptyList();
        }
        Collection<SQLToken> result = new LinkedList<>();
        for (SimpleTableSegment each : allTables) {
            TableNameSegment tableName = each.getTableName();
            if (broadcastRule.get().getAttributes().getAttribute(TableMapperRuleAttribute.class).getLogicTableNames().contains(tableName.getIdentifier().getValue())) {
                result.add(new BroadcastTableToken(tableName.getStartIndex(), tableName.getStopIndex(), tableName.getIdentifier(), database, sqlStatementContext.getDatabaseType()));
            }
        }
        return result;
    }
}
