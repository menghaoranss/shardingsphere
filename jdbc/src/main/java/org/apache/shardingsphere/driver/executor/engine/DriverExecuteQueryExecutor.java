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

package org.apache.shardingsphere.driver.executor.engine;

import com.google.common.base.Strings;
import com.sphereex.dbplusengine.SphereEx;
import com.sphereex.dbplusengine.driver.executor.physical.PhysicalExecuteQueryExecutor;
import com.sphereex.dbplusengine.infra.binder.engine.dialect.SystemSchemaQueryDetector;
import org.apache.shardingsphere.driver.executor.callback.add.StatementAddCallback;
import org.apache.shardingsphere.driver.executor.callback.replay.StatementReplayCallback;
import org.apache.shardingsphere.driver.executor.engine.pushdown.jdbc.DriverJDBCPushDownExecuteQueryExecutor;
import org.apache.shardingsphere.driver.executor.engine.pushdown.raw.DriverRawPushDownExecuteQueryExecutor;
import org.apache.shardingsphere.driver.jdbc.core.connection.ShardingSphereConnection;
import org.apache.shardingsphere.infra.binder.context.statement.dml.SelectStatementContext;
import org.apache.shardingsphere.infra.database.core.spi.DatabaseTypedSPILoader;
import org.apache.shardingsphere.infra.exception.kernel.metadata.resource.storageunit.EmptyStorageUnitException;
import org.apache.shardingsphere.infra.executor.sql.execute.engine.driver.jdbc.JDBCExecutionUnit;
import org.apache.shardingsphere.infra.executor.sql.execute.engine.driver.jdbc.JDBCExecutor;
import org.apache.shardingsphere.infra.executor.sql.execute.engine.raw.RawExecutor;
import org.apache.shardingsphere.infra.executor.sql.prepare.driver.DriverExecutionPrepareEngine;
import org.apache.shardingsphere.infra.metadata.ShardingSphereMetaData;
import org.apache.shardingsphere.infra.metadata.database.ShardingSphereDatabase;
import org.apache.shardingsphere.infra.metadata.database.schema.manager.SystemSchemaManager;
import org.apache.shardingsphere.infra.rule.attribute.raw.RawExecutionRuleAttribute;
import org.apache.shardingsphere.infra.session.query.QueryContext;
import org.apache.shardingsphere.sql.parser.statement.core.segment.generic.OwnerSegment;
import org.apache.shardingsphere.sql.parser.statement.core.segment.generic.table.SimpleTableSegment;
import org.apache.shardingsphere.sql.parser.statement.core.value.identifier.IdentifierValue;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.Optional;

/**
 * Driver execute query executor.
 */
public final class DriverExecuteQueryExecutor {
    
    private final ShardingSphereConnection connection;
    
    private final ShardingSphereMetaData metaData;
    
    private final DriverJDBCPushDownExecuteQueryExecutor driverJDBCPushDownExecutor;
    
    private final DriverRawPushDownExecuteQueryExecutor driverRawPushDownExecutor;
    
    public DriverExecuteQueryExecutor(final ShardingSphereConnection connection, final ShardingSphereMetaData metaData, final JDBCExecutor jdbcExecutor, final RawExecutor rawExecutor) {
        this.connection = connection;
        this.metaData = metaData;
        driverJDBCPushDownExecutor = new DriverJDBCPushDownExecuteQueryExecutor(connection, metaData, jdbcExecutor);
        driverRawPushDownExecutor = new DriverRawPushDownExecuteQueryExecutor(connection, metaData, rawExecutor);
    }
    
    /**
     * Execute query.
     *
     * @param database database
     * @param queryContext query context
     * @param prepareEngine prepare engine
     * @param statement statement
     * @param columnLabelAndIndexMap column label and index map
     * @param addCallback statement add callback
     * @param replayCallback statement replay callback
     * @return result set
     * @throws SQLException SQL exception
     */
    @SuppressWarnings("rawtypes")
    public ResultSet executeQuery(final ShardingSphereDatabase database, final QueryContext queryContext,
                                  final DriverExecutionPrepareEngine<JDBCExecutionUnit, Connection> prepareEngine, final Statement statement, final Map<String, Integer> columnLabelAndIndexMap,
                                  final StatementAddCallback addCallback, final StatementReplayCallback replayCallback) throws SQLException {
        // SPEX ADDED: BEGIN
        if (isQuerySystemSchema(queryContext)) {
            ShardingSphereDatabase queryDatabase = database.getResourceMetaData().getStorageUnits().isEmpty() ? findHasStorageUnitsDatabase().orElse(null) : database;
            if (null == queryDatabase) {
                throw new EmptyStorageUnitException();
            }
            PhysicalExecuteQueryExecutor physicalQueryExecutor = new PhysicalExecuteQueryExecutor(queryDatabase.getName(), connection.getDatabaseConnectionManager());
            physicalQueryExecutor.executeQuery(queryContext, queryDatabase.getResourceMetaData().getStorageUnits().keySet().iterator().next());
            return physicalQueryExecutor.getResultSet();
        }
        // SPEX ADDED: END
        return database.getRuleMetaData().getAttributes(RawExecutionRuleAttribute.class).isEmpty()
                ? driverJDBCPushDownExecutor.executeQuery(database, queryContext, prepareEngine, statement, columnLabelAndIndexMap, addCallback, replayCallback)
                : driverRawPushDownExecutor.executeQuery(database, queryContext, statement, columnLabelAndIndexMap);
    }
    
    @SphereEx
    private boolean isQuerySystemSchema(final QueryContext queryContext) {
        Optional<SystemSchemaQueryDetector> systemSchemaQueryDetector = DatabaseTypedSPILoader.findService(SystemSchemaQueryDetector.class, queryContext.getSqlStatementContext().getDatabaseType());
        if (systemSchemaQueryDetector.isPresent()) {
            return systemSchemaQueryDetector.get().isSystemSchemaQuery(queryContext.getSqlStatementContext());
        }
        if (!(queryContext.getSqlStatementContext() instanceof SelectStatementContext)) {
            return false;
        }
        for (SimpleTableSegment each : ((SelectStatementContext) queryContext.getSqlStatementContext()).getTablesContext().getSimpleTables()) {
            String owner = each.getOwner().map(OwnerSegment::getIdentifier).map(IdentifierValue::getValue).orElse(null);
            String tableName = each.getTableName().getIdentifier().getValue();
            if (!Strings.isNullOrEmpty(owner) && SystemSchemaManager.isSystemTable(queryContext.getSqlStatementContext().getDatabaseType().getType(), owner, tableName)) {
                return true;
            }
        }
        return false;
    }
    
    @SphereEx
    private Optional<ShardingSphereDatabase> findHasStorageUnitsDatabase() {
        for (ShardingSphereDatabase each : metaData.getAllDatabases()) {
            if (!each.getResourceMetaData().getStorageUnits().keySet().isEmpty()) {
                return Optional.of(each);
            }
        }
        return Optional.empty();
    }
}
