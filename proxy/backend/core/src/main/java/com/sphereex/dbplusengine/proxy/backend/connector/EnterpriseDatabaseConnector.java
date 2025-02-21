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

package com.sphereex.dbplusengine.proxy.backend.connector;

import com.sphereex.dbplusengine.driver.executor.physical.PhysicalExecuteQueryExecutor;
import com.sphereex.dbplusengine.infra.rule.attribute.failover.FailOverRuleAttribute;
import lombok.extern.slf4j.Slf4j;
import org.apache.shardingsphere.infra.executor.sql.execute.result.query.impl.driver.jdbc.metadata.JDBCQueryResultMetaData;
import org.apache.shardingsphere.infra.executor.sql.execute.result.query.impl.driver.jdbc.type.stream.JDBCStreamQueryResult;
import org.apache.shardingsphere.infra.merge.result.MergedResult;
import org.apache.shardingsphere.infra.metadata.database.ShardingSphereDatabase;
import org.apache.shardingsphere.infra.session.query.QueryContext;
import org.apache.shardingsphere.proxy.backend.connector.DatabaseConnector;
import org.apache.shardingsphere.proxy.backend.connector.ProxyDatabaseConnectionManager;
import org.apache.shardingsphere.proxy.backend.connector.StandardDatabaseConnector;
import org.apache.shardingsphere.proxy.backend.response.data.QueryResponseCell;
import org.apache.shardingsphere.proxy.backend.response.data.QueryResponseRow;
import org.apache.shardingsphere.proxy.backend.response.header.ResponseHeader;
import org.apache.shardingsphere.proxy.backend.response.header.query.QueryHeader;
import org.apache.shardingsphere.proxy.backend.response.header.query.QueryHeaderBuilderEngine;
import org.apache.shardingsphere.proxy.backend.response.header.query.QueryResponseHeader;
import org.apache.shardingsphere.sharding.merge.common.IteratorStreamMergedResult;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * Enterprise database connector.
 */
@Slf4j
public final class EnterpriseDatabaseConnector implements DatabaseConnector {
    
    private final StandardDatabaseConnector delegate;
    
    private final PhysicalExecuteQueryExecutor physicalQueryExecutor;
    
    private List<QueryHeader> queryHeaders;
    
    private MergedResult mergedResult;
    
    public EnterpriseDatabaseConnector(final String driverType, final QueryContext queryContext, final ProxyDatabaseConnectionManager databaseConnectionManager) {
        delegate = new StandardDatabaseConnector(driverType, queryContext, databaseConnectionManager);
        physicalQueryExecutor = new PhysicalExecuteQueryExecutor(databaseConnectionManager.getConnectionSession().getCurrentDatabaseName(), delegate.getDatabaseConnectionManager());
    }
    
    @Override
    public void add(final Statement statement) {
        delegate.add(statement);
    }
    
    @Override
    public void add(final ResultSet resultSet) {
        delegate.add(resultSet);
    }
    
    @Override
    public ResponseHeader execute() throws SQLException {
        try {
            return delegate.execute();
            // CHECKSTYLE:OFF
        } catch (final Exception ex) {
            // CHECKSTYLE:ON
            ShardingSphereDatabase database = delegate.getDatabase();
            Optional<FailOverRuleAttribute> attribute = database.getRuleMetaData().findAttribute(FailOverRuleAttribute.class);
            // TODO consider optimize this logic when multi FailOverRuleAttribute use together
            QueryContext queryContext = delegate.getQueryContext();
            if (attribute.isPresent() && attribute.get().isNeedFailOver(queryContext.getSqlStatementContext().getSqlStatement())) {
                ResultSet resultSet = executeFailOverSQL(queryContext, ex, attribute.get().getFailOverDataSourceName(database.getResourceMetaData()));
                return processExecuteResultSet(resultSet, database);
            }
            throw ex;
        }
    }
    
    private ResultSet executeFailOverSQL(final QueryContext queryContext, final Exception originalEx, final String failOverDataSourceName) throws SQLException {
        try {
            ResultSet result = physicalQueryExecutor.executeQuery(queryContext, failOverDataSourceName);
            log.warn("Execute fail over SQL success: {} ::: {} ::: {}", failOverDataSourceName, queryContext.getSql(), queryContext.getParameters(), originalEx);
            return result;
            // CHECKSTYLE:OFF
        } catch (final Exception ex) {
            // CHECKSTYLE:ON
            log.warn("Execute fail over SQL failed: {} ::: {} ::: {}", failOverDataSourceName, queryContext.getSql(), queryContext.getParameters(), ex);
            throw new SQLException(originalEx);
        }
    }
    
    private ResponseHeader processExecuteResultSet(final ResultSet resultSet, final ShardingSphereDatabase database) throws SQLException {
        int columnCount = resultSet.getMetaData().getColumnCount();
        queryHeaders = new ArrayList<>(columnCount);
        QueryHeaderBuilderEngine queryHeaderBuilderEngine = new QueryHeaderBuilderEngine(null == database ? null : database.getProtocolType());
        for (int columnIndex = 1; columnIndex <= columnCount; columnIndex++) {
            queryHeaders.add(queryHeaderBuilderEngine.build(new JDBCQueryResultMetaData(resultSet.getMetaData()), database, columnIndex));
        }
        mergedResult = new IteratorStreamMergedResult(Collections.singletonList(new JDBCStreamQueryResult(resultSet)));
        return new QueryResponseHeader(queryHeaders);
    }
    
    @Override
    public boolean next() throws SQLException {
        return null != mergedResult ? mergedResult.next() : delegate.next();
    }
    
    @Override
    public QueryResponseRow getRowData() throws SQLException {
        if (null == mergedResult) {
            return delegate.getRowData();
        }
        List<QueryResponseCell> cells = new ArrayList<>(queryHeaders.size());
        for (int columnIndex = 1; columnIndex <= queryHeaders.size(); columnIndex++) {
            Object data = mergedResult.getValue(columnIndex, Object.class);
            cells.add(new QueryResponseCell(queryHeaders.get(columnIndex - 1).getColumnType(), data, queryHeaders.get(columnIndex - 1).getColumnTypeName()));
        }
        return new QueryResponseRow(cells);
    }
    
    @Override
    public void close() throws SQLException {
        delegate.close();
    }
}
