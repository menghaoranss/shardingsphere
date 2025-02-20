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

package com.sphereex.dbplusengine.driver.executor.physical;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.apache.shardingsphere.infra.executor.sql.execute.engine.ConnectionMode;
import org.apache.shardingsphere.infra.executor.sql.prepare.driver.DatabaseConnectionManager;
import org.apache.shardingsphere.infra.session.query.QueryContext;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

/**
 * Physical execute query executor.
 */
@RequiredArgsConstructor
public final class PhysicalExecuteQueryExecutor implements AutoCloseable {
    
    private final String currentDatabaseName;
    
    private final DatabaseConnectionManager<Connection> connectionManager;
    
    private PreparedStatement preparedStatement;
    
    @Getter
    private ResultSet resultSet;
    
    /**
     * Execute query.
     *
     * @param queryContext query context
     * @param failOverDataSourceName fail over data source name
     * @return result set
     * @throws SQLException SQL exception
     */
    public ResultSet executeQuery(final QueryContext queryContext, final String failOverDataSourceName) throws SQLException {
        List<Connection> connections = connectionManager.getConnections(currentDatabaseName, failOverDataSourceName, 0, 1, ConnectionMode.MEMORY_STRICTLY);
        PreparedStatement preparedStatement = connections.iterator().next().prepareStatement(queryContext.getSql());
        this.preparedStatement = preparedStatement;
        setParameters(preparedStatement, queryContext.getParameters());
        ResultSet resultSet = preparedStatement.executeQuery();
        this.resultSet = resultSet;
        return resultSet;
    }
    
    private void setParameters(final PreparedStatement preparedStatement, final List<Object> parameters) throws SQLException {
        int index = 1;
        for (Object each : parameters) {
            preparedStatement.setObject(index++, each);
        }
    }
    
    @Override
    public void close() throws Exception {
        if (null != resultSet) {
            resultSet.close();
        }
        if (null != preparedStatement) {
            preparedStatement.close();
        }
    }
}
