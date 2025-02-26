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

package org.apache.shardingsphere.proxy.backend.handler.distsql.rul;

import com.google.common.base.Preconditions;
import lombok.Setter;
import org.apache.shardingsphere.distsql.handler.aware.DistSQLExecutorConnectionContextAware;
import org.apache.shardingsphere.distsql.handler.aware.DistSQLExecutorDatabaseAware;
import org.apache.shardingsphere.distsql.handler.engine.DistSQLConnectionContext;
import org.apache.shardingsphere.distsql.handler.engine.query.DistSQLQueryExecutor;
import org.apache.shardingsphere.distsql.statement.rul.sql.PreviewStatement;
import org.apache.shardingsphere.infra.binder.context.aware.CursorAware;
import org.apache.shardingsphere.infra.binder.context.statement.SQLStatementContext;
import org.apache.shardingsphere.infra.binder.context.statement.ddl.CursorStatementContext;
import org.apache.shardingsphere.infra.binder.context.type.CursorAvailable;
import org.apache.shardingsphere.infra.binder.engine.SQLBindEngine;
import org.apache.shardingsphere.infra.connection.kernel.KernelProcessor;
import org.apache.shardingsphere.infra.exception.core.ShardingSpherePreconditions;
import org.apache.shardingsphere.infra.exception.kernel.metadata.rule.EmptyRuleException;
import org.apache.shardingsphere.infra.executor.sql.context.ExecutionUnit;
import org.apache.shardingsphere.infra.hint.HintValueContext;
import org.apache.shardingsphere.infra.merge.result.impl.local.LocalDataQueryResultRow;
import org.apache.shardingsphere.infra.metadata.ShardingSphereMetaData;
import org.apache.shardingsphere.infra.metadata.database.ShardingSphereDatabase;
import org.apache.shardingsphere.infra.session.query.QueryContext;
import org.apache.shardingsphere.mode.manager.ContextManager;
import org.apache.shardingsphere.parser.rule.SQLParserRule;
import org.apache.shardingsphere.sql.parser.statement.core.statement.SQLStatement;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.stream.Collectors;

/**
 * Preview executor.
 */
@Setter
public final class PreviewExecutor implements DistSQLQueryExecutor<PreviewStatement>, DistSQLExecutorDatabaseAware, DistSQLExecutorConnectionContextAware {
    
    private ShardingSphereDatabase database;
    
    private DistSQLConnectionContext connectionContext;
    
    @Override
    public Collection<String> getColumnNames(final PreviewStatement sqlStatement) {
        return Arrays.asList("data_source_name", "actual_sql");
    }
    
    @Override
    public Collection<LocalDataQueryResultRow> getRows(final PreviewStatement sqlStatement, final ContextManager contextManager) throws SQLException {
        ShardingSphereMetaData metaData = contextManager.getMetaDataContexts().getMetaData();
        String toBePreviewedSQL = sqlStatement.getSql();
        SQLStatement toBePreviewedStatement = metaData.getGlobalRuleMetaData().getSingleRule(SQLParserRule.class).getSQLParserEngine(database.getProtocolType()).parse(toBePreviewedSQL, false);
        HintValueContext hintValueContext = connectionContext.getQueryContext().getHintValueContext();
        hintValueContext.setSkipMetadataValidate(true);
        String currentDatabaseName = connectionContext.getQueryContext().getConnectionContext().getCurrentDatabaseName().orElse(null);
        SQLStatementContext toBePreviewedStatementContext = new SQLBindEngine(metaData, currentDatabaseName, hintValueContext).bind(toBePreviewedStatement, Collections.emptyList());
        QueryContext queryContext =
                new QueryContext(toBePreviewedStatementContext, toBePreviewedSQL, Collections.emptyList(), hintValueContext, connectionContext.getQueryContext().getConnectionContext(), metaData);
        if (toBePreviewedStatementContext instanceof CursorAvailable && toBePreviewedStatementContext instanceof CursorAware) {
            setUpCursorDefinition(toBePreviewedStatementContext);
        }
        ShardingSpherePreconditions.checkState(database.isComplete(), () -> new EmptyRuleException(database.getName()));
        Collection<ExecutionUnit> executionUnits = getExecutionUnits(metaData, queryContext);
        return executionUnits.stream().map(each -> new LocalDataQueryResultRow(each.getDataSourceName(), each.getSqlUnit().getSql())).collect(Collectors.toList());
    }
    
    private Collection<ExecutionUnit> getExecutionUnits(final ShardingSphereMetaData metaData, final QueryContext queryContext) {
        return new KernelProcessor().generateExecutionContext(queryContext, metaData.getGlobalRuleMetaData(), metaData.getProps()).getExecutionUnits();
    }
    
    private void setUpCursorDefinition(final SQLStatementContext toBePreviewedStatementContext) {
        if (!((CursorAvailable) toBePreviewedStatementContext).getCursorName().isPresent()) {
            return;
        }
        String cursorName = ((CursorAvailable) toBePreviewedStatementContext).getCursorName().get().getIdentifier().getValue().toLowerCase();
        CursorStatementContext cursorStatementContext = connectionContext.getQueryContext().getConnectionContext().getCursorContext().getCursorStatementContexts().get(cursorName);
        Preconditions.checkNotNull(cursorStatementContext, "Cursor %s does not exist.", cursorName);
        ((CursorAware) toBePreviewedStatementContext).setCursorStatementContext(cursorStatementContext);
    }
    
    @Override
    public Class<PreviewStatement> getType() {
        return PreviewStatement.class;
    }
}
