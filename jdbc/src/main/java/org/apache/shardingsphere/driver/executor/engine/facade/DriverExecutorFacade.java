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

package org.apache.shardingsphere.driver.executor.engine.facade;

import com.sphereex.dbplusengine.SphereEx;
import com.sphereex.dbplusengine.SphereEx.Type;
import com.sphereex.dbplusengine.driver.executor.physical.PhysicalExecuteQueryExecutor;
import com.sphereex.dbplusengine.infra.rule.attribute.failover.FailOverRuleAttribute;
import lombok.extern.slf4j.Slf4j;
import org.apache.shardingsphere.driver.executor.callback.add.StatementAddCallback;
import org.apache.shardingsphere.driver.executor.callback.execute.StatementExecuteCallback;
import org.apache.shardingsphere.driver.executor.callback.execute.StatementExecuteUpdateCallback;
import org.apache.shardingsphere.driver.executor.callback.replay.StatementReplayCallback;
import org.apache.shardingsphere.driver.executor.engine.DriverExecuteExecutor;
import org.apache.shardingsphere.driver.executor.engine.DriverExecuteQueryExecutor;
import org.apache.shardingsphere.driver.executor.engine.DriverExecuteUpdateExecutor;
import org.apache.shardingsphere.driver.executor.engine.transaction.DriverTransactionSQLStatementExecutor;
import org.apache.shardingsphere.driver.jdbc.core.connection.ShardingSphereConnection;
import org.apache.shardingsphere.driver.jdbc.core.statement.StatementManager;
import org.apache.shardingsphere.infra.binder.context.statement.SQLStatementContext;
import org.apache.shardingsphere.infra.config.props.ConfigurationPropertyKey;
import org.apache.shardingsphere.infra.executor.audit.SQLAuditEngine;
import org.apache.shardingsphere.infra.executor.sql.execute.engine.driver.jdbc.JDBCExecutionUnit;
import org.apache.shardingsphere.infra.executor.sql.execute.engine.driver.jdbc.JDBCExecutor;
import org.apache.shardingsphere.infra.executor.sql.execute.engine.raw.RawExecutor;
import org.apache.shardingsphere.infra.executor.sql.prepare.driver.DriverExecutionPrepareEngine;
import org.apache.shardingsphere.infra.executor.sql.prepare.driver.jdbc.StatementOption;
import org.apache.shardingsphere.infra.metadata.ShardingSphereMetaData;
import org.apache.shardingsphere.infra.metadata.database.ShardingSphereDatabase;
import org.apache.shardingsphere.infra.session.query.QueryContext;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Driver executor facade.
 */
// SPEX ADDED: BEGIN
@Slf4j
// SPEX ADDED: END
public final class DriverExecutorFacade implements AutoCloseable {
    
    private final ShardingSphereConnection connection;
    
    private final StatementOption statementOption;
    
    private final StatementManager statementManager;
    
    private final String jdbcDriverType;
    
    private final DriverTransactionSQLStatementExecutor transactionExecutor;
    
    private final DriverExecuteQueryExecutor queryExecutor;
    
    private final DriverExecuteUpdateExecutor updateExecutor;
    
    private final DriverExecuteExecutor executeExecutor;
    
    @SphereEx
    private final PhysicalExecuteQueryExecutor physicalQueryExecutor;
    
    public DriverExecutorFacade(final ShardingSphereConnection connection, final StatementOption statementOption, final StatementManager statementManager, final String jdbcDriverType) {
        this.connection = connection;
        this.statementOption = statementOption;
        this.statementManager = statementManager;
        this.jdbcDriverType = jdbcDriverType;
        JDBCExecutor jdbcExecutor = new JDBCExecutor(connection.getContextManager().getExecutorEngine(), connection.getDatabaseConnectionManager().getConnectionContext());
        ShardingSphereMetaData metaData = connection.getContextManager().getMetaDataContexts().getMetaData();
        transactionExecutor = new DriverTransactionSQLStatementExecutor(connection);
        RawExecutor rawExecutor = new RawExecutor(connection.getContextManager().getExecutorEngine(), connection.getDatabaseConnectionManager().getConnectionContext());
        queryExecutor = new DriverExecuteQueryExecutor(connection, metaData, jdbcExecutor, rawExecutor);
        updateExecutor = new DriverExecuteUpdateExecutor(connection, metaData, jdbcExecutor, rawExecutor);
        executeExecutor = new DriverExecuteExecutor(connection, metaData, jdbcExecutor, rawExecutor, transactionExecutor);
        // SPEX ADDED: BEGIN
        physicalQueryExecutor = new PhysicalExecuteQueryExecutor(connection.getCurrentDatabaseName(), connection.getDatabaseConnectionManager());
        // SPEX ADDED: END
    }
    
    /**
     * Execute query.
     *
     * @param database database
     * @param queryContext query context
     * @param statement statement
     * @param columnLabelAndIndexMap column label and index map
     * @param addCallback statement add callback
     * @param replayCallback statement replay callback
     * @return result set
     * @throws SQLException SQL exception
     */
    @SuppressWarnings("rawtypes")
    public ResultSet executeQuery(final ShardingSphereDatabase database, final QueryContext queryContext, final Statement statement, final Map<String, Integer> columnLabelAndIndexMap,
                                  final StatementAddCallback addCallback, final StatementReplayCallback replayCallback) throws SQLException {
        // SPEX ADDED: BEGIN
        try {
            // SPEX ADDED: END
            SQLAuditEngine.audit(queryContext, connection.getContextManager().getMetaDataContexts().getMetaData().getGlobalRuleMetaData(), database);
            return queryExecutor.executeQuery(database, queryContext, createDriverExecutionPrepareEngine(database, jdbcDriverType), statement, columnLabelAndIndexMap, addCallback, replayCallback);
            // SPEX ADDED: BEGIN
            // CHECKSTYLE:OFF
        } catch (final Exception ex) {
            // CHECKSTYLE:ON
            Optional<FailOverRuleAttribute> attribute = database.getRuleMetaData().findAttribute(FailOverRuleAttribute.class);
            // TODO consider optimize this logic when multi FailOverRuleAttribute use together
            if (attribute.isPresent() && attribute.get().isNeedFailOver(queryContext.getSqlStatementContext().getSqlStatement())) {
                return executeFailOverSQL(queryContext, ex, attribute.get().getFailOverDataSourceName(database.getResourceMetaData()));
            }
            throw ex;
        }
        // SPEX ADDED: END
    }
    
    @SphereEx
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
    
    /**
     * Execute update.
     *
     * @param database database
     * @param queryContext query context
     * @param executeUpdateCallback statement execute update callback
     * @param replayCallback statement replay callback
     * @param addCallback statement add callback
     * @return updated row count
     * @throws SQLException SQL exception
     */
    @SuppressWarnings("rawtypes")
    public int executeUpdate(final ShardingSphereDatabase database, final QueryContext queryContext,
                             final StatementExecuteUpdateCallback executeUpdateCallback, final StatementAddCallback addCallback, final StatementReplayCallback replayCallback) throws SQLException {
        SQLAuditEngine.audit(queryContext, connection.getContextManager().getMetaDataContexts().getMetaData().getGlobalRuleMetaData(), database);
        return updateExecutor.executeUpdate(database, queryContext, createDriverExecutionPrepareEngine(database, jdbcDriverType), executeUpdateCallback, addCallback, replayCallback);
    }
    
    /**
     * Execute.
     *
     * @param database database
     * @param queryContext query context
     * @param executeCallback statement execute callback
     * @param addCallback statement add callback
     * @param replayCallback statement replay callback
     * @return execute result
     * @throws SQLException SQL exception
     */
    @SuppressWarnings("rawtypes")
    public boolean execute(final ShardingSphereDatabase database, final QueryContext queryContext,
                           final StatementExecuteCallback executeCallback, final StatementAddCallback addCallback, final StatementReplayCallback replayCallback) throws SQLException {
        // SPEX ADDED: BEGIN
        try {
            // SPEX ADDED: END
            SQLAuditEngine.audit(queryContext, connection.getContextManager().getMetaDataContexts().getMetaData().getGlobalRuleMetaData(), database);
            return executeExecutor.execute(database, queryContext, createDriverExecutionPrepareEngine(database, jdbcDriverType), executeCallback, addCallback, replayCallback);
            // SPEX ADDED: BEGIN
            // CHECKSTYLE:OFF
        } catch (final Exception ex) {
            // CHECKSTYLE:ON
            Optional<FailOverRuleAttribute> attribute = database.getRuleMetaData().findAttribute(FailOverRuleAttribute.class);
            // TODO consider optimize this logic when multi FailOverRuleAttribute use together
            if (attribute.isPresent() && attribute.get().isNeedFailOver(queryContext.getSqlStatementContext().getSqlStatement())) {
                return null != executeFailOverSQL(queryContext, ex, attribute.get().getFailOverDataSourceName(database.getResourceMetaData()));
            }
            throw ex;
        }
        // SPEX ADDED: END
    }
    
    private DriverExecutionPrepareEngine<JDBCExecutionUnit, Connection> createDriverExecutionPrepareEngine(final ShardingSphereDatabase database, final String jdbcDriverType) {
        int maxConnectionsSizePerQuery = connection.getContextManager().getMetaDataContexts().getMetaData().getProps().<Integer>getValue(ConfigurationPropertyKey.MAX_CONNECTIONS_SIZE_PER_QUERY);
        return new DriverExecutionPrepareEngine<>(jdbcDriverType, maxConnectionsSizePerQuery, connection.getDatabaseConnectionManager(), statementManager, statementOption,
                database.getRuleMetaData().getRules(), database.getResourceMetaData().getStorageUnits());
    }
    
    /**
     * Get result set.
     *
     * @param database database
     * @param sqlStatementContext SQL statement context
     * @param statement statement
     * @param statements statements
     * @return result set
     * @throws SQLException SQL exception
     */
    @SphereEx(Type.MODIFY)
    public Optional<ResultSet> getResultSet(final ShardingSphereDatabase database,
                                            final SQLStatementContext sqlStatementContext, final Statement statement, final List<? extends Statement> statements) throws SQLException {
        Optional<ResultSet> resultSet = executeExecutor.getResultSet(database, sqlStatementContext, statement, statements);
        if (resultSet.isPresent()) {
            return resultSet;
        }
        return Optional.ofNullable(physicalQueryExecutor.getResultSet());
    }
    
    @Override
    public void close() throws SQLException {
    }
}
