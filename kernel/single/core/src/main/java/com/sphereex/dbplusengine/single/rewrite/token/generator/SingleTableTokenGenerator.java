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

package com.sphereex.dbplusengine.single.rewrite.token.generator;

import com.cedarsoftware.util.CaseInsensitiveMap;
import com.sphereex.dbplusengine.infra.rewrite.aware.GlobalMetaDataAware;
import com.sphereex.dbplusengine.infra.rewrite.token.TableTokenUtils;
import com.sphereex.dbplusengine.single.rewrite.token.pojo.SingleTableToken;
import lombok.Setter;
import org.apache.shardingsphere.infra.binder.context.aware.CursorAware;
import org.apache.shardingsphere.infra.binder.context.statement.SQLStatementContext;
import org.apache.shardingsphere.infra.binder.context.statement.dml.SelectStatementContext;
import org.apache.shardingsphere.infra.binder.context.type.TableAvailable;
import org.apache.shardingsphere.infra.database.mysql.type.MySQLDatabaseType;
import org.apache.shardingsphere.infra.database.oracle.type.OracleDatabaseType;
import org.apache.shardingsphere.infra.metadata.ShardingSphereMetaData;
import org.apache.shardingsphere.infra.metadata.database.ShardingSphereDatabase;
import org.apache.shardingsphere.infra.metadata.database.schema.QualifiedTable;
import org.apache.shardingsphere.infra.rewrite.sql.token.common.generator.CollectionSQLTokenGenerator;
import org.apache.shardingsphere.infra.rewrite.sql.token.common.generator.aware.ConnectionContextAware;
import org.apache.shardingsphere.infra.rewrite.sql.token.common.pojo.SQLToken;
import org.apache.shardingsphere.infra.session.connection.ConnectionContext;
import org.apache.shardingsphere.single.rule.SingleRule;
import org.apache.shardingsphere.sql.parser.statement.core.segment.generic.table.SimpleTableSegment;
import org.apache.shardingsphere.sql.parser.statement.core.segment.generic.table.TableNameSegment;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.Map;
import java.util.Optional;

/**
 * Single table token generator.
 */
@Setter
public final class SingleTableTokenGenerator implements CollectionSQLTokenGenerator<SQLStatementContext>, GlobalMetaDataAware, ConnectionContextAware {
    
    private Collection<SimpleTableSegment> allTables;
    
    private ShardingSphereMetaData metaData;
    
    private ConnectionContext connectionContext;
    
    private ShardingSphereDatabase currentDatabase;
    
    private SingleRule currentSingleRule;
    
    @Override
    public boolean isGenerateSQLToken(final SQLStatementContext sqlStatementContext) {
        if (!(sqlStatementContext instanceof TableAvailable) || sqlStatementContext instanceof CursorAware) {
            return false;
        }
        boolean isNeedRewriteDatabaseType = sqlStatementContext.getDatabaseType() instanceof MySQLDatabaseType || sqlStatementContext.getDatabaseType() instanceof OracleDatabaseType;
        if (!isNeedRewriteDatabaseType) {
            return false;
        }
        allTables = ((TableAvailable) sqlStatementContext).getTablesContext().getSimpleTables();
        return !allTables.isEmpty();
    }
    
    @Override
    public Collection<SQLToken> generateSQLTokens(final SQLStatementContext sqlStatementContext) {
        currentDatabase = connectionContext.getCurrentDatabaseName().map(currentDatabaseName -> metaData.getDatabase(currentDatabaseName))
                .orElseThrow(() -> new IllegalStateException("Current database can not be null"));
        currentSingleRule = currentDatabase.getRuleMetaData().getSingleRule(SingleRule.class);
        if (!TableTokenUtils.isNeedSchemaRewrite(currentDatabase.getResourceMetaData().getStorageUnits())) {
            return Collections.emptyList();
        }
        Collection<SQLToken> result = new LinkedList<>();
        Map<String, QualifiedTable> currentDatabaseSingleTables = getSingleTables(sqlStatementContext, currentDatabase, currentSingleRule);
        for (SimpleTableSegment each : allTables) {
            if (isUsedCTETable(sqlStatementContext, each)) {
                continue;
            }
            getSingleTableToken(sqlStatementContext, each, currentDatabaseSingleTables).ifPresent(result::add);
        }
        return result;
    }
    
    private boolean isUsedCTETable(final SQLStatementContext sqlStatementContext, final SimpleTableSegment simpleTableSegment) {
        if (!(sqlStatementContext instanceof SelectStatementContext)) {
            return false;
        }
        SelectStatementContext selectStatementContext = (SelectStatementContext) sqlStatementContext;
        if (!selectStatementContext.getSqlStatement().getWithSegment().isPresent()) {
            return false;
        }
        if (selectStatementContext.getSqlStatement().getWithSegment().get().getStartIndex() <= simpleTableSegment.getStartIndex()
                && selectStatementContext.getSqlStatement().getWithSegment().get().getStopIndex() >= simpleTableSegment.getStopIndex()) {
            return false;
        }
        return selectStatementContext.getCommonTableExpressionAliases().contains(simpleTableSegment.getTableName().getIdentifier().getValue());
    }
    
    private Optional<SingleTableToken> getSingleTableToken(final SQLStatementContext sqlStatementContext, final SimpleTableSegment tableSegment,
                                                           final Map<String, QualifiedTable> currentDatabaseSingleTables) {
        TableNameSegment tableName = tableSegment.getTableName();
        Optional<String> databaseName = TableTokenUtils.getDatabaseName(tableSegment, sqlStatementContext.getDatabaseType());
        if (databaseName.isPresent() && !databaseName.get().equalsIgnoreCase(currentDatabase.getName())) {
            ShardingSphereDatabase targetDatabase = metaData.getDatabase(databaseName.get());
            Optional<SingleRule> targetSingleRule = targetDatabase.getRuleMetaData().findSingleRule(SingleRule.class);
            if (!targetSingleRule.isPresent()) {
                return Optional.empty();
            }
            Map<String, QualifiedTable> targetDatabaseSingleTables = getSingleTables(sqlStatementContext, targetDatabase, targetSingleRule.get());
            return getSingleTableToken(targetDatabaseSingleTables, tableName, targetDatabase);
        } else {
            return getSingleTableToken(currentDatabaseSingleTables, tableName, currentDatabase);
        }
    }
    
    private Optional<SingleTableToken> getSingleTableToken(final Map<String, QualifiedTable> singleTables, final TableNameSegment tableName, final ShardingSphereDatabase database) {
        QualifiedTable qualifiedTable = singleTables.get(tableName.getIdentifier().getValue());
        if (null == qualifiedTable) {
            return Optional.empty();
        }
        return Optional.of(new SingleTableToken(tableName.getStartIndex(), tableName.getStopIndex(), tableName.getIdentifier(), database, metaData));
    }
    
    private Map<String, QualifiedTable> getSingleTables(final SQLStatementContext sqlStatementContext, final ShardingSphereDatabase database, final SingleRule singleRule) {
        Collection<QualifiedTable> qualifiedTables = singleRule.getQualifiedTables(sqlStatementContext, database);
        Map<String, QualifiedTable> result = new CaseInsensitiveMap<>(qualifiedTables.size(), 1F);
        for (QualifiedTable each : singleRule.getSingleTables(qualifiedTables)) {
            result.put(each.getTableName(), each);
        }
        return result;
    }
}
