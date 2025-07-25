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

package org.apache.shardingsphere.infra.route.postgresql;

import org.apache.shardingsphere.infra.route.engine.tableless.DialectDALStatementBroadcastRouteDecider;
import org.apache.shardingsphere.sql.parser.statement.core.statement.type.dal.DALStatement;
import org.apache.shardingsphere.sql.parser.statement.postgresql.dal.PostgreSQLLoadStatement;
import org.apache.shardingsphere.sql.parser.statement.postgresql.dal.PostgreSQLResetParameterStatement;

/**
 * Dialect DAL statement broadcast route decider for PostgreSQL.
 */
public final class PostgreSQLDALStatementBroadcastRouteDecider implements DialectDALStatementBroadcastRouteDecider {
    
    @Override
    public boolean isDataSourceBroadcastRoute(final DALStatement sqlStatement) {
        return sqlStatement instanceof PostgreSQLResetParameterStatement || sqlStatement instanceof PostgreSQLLoadStatement;
    }
    
    @Override
    public boolean isInstanceBroadcastRoute(final DALStatement sqlStatement) {
        return false;
    }
    
    @Override
    public String getDatabaseType() {
        return "PostgreSQL";
    }
}
