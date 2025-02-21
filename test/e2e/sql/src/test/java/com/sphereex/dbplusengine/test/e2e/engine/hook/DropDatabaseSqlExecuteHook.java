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

package com.sphereex.dbplusengine.test.e2e.engine.hook;

import com.sphereex.dbplusengine.test.e2e.engine.context.ComparisonDataSourceContext;
import lombok.SneakyThrows;
import org.apache.shardingsphere.sql.parser.statement.core.statement.SQLStatement;
import org.apache.shardingsphere.sql.parser.statement.core.statement.ddl.DropDatabaseStatement;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Objects;

public class DropDatabaseSqlExecuteHook extends AbstratParserSqlExecuteHook {
    
    @SneakyThrows(SQLException.class)
    @Override
    public void after(final String sql, final ComparisonDataSourceContext context) {
        SQLStatement sqlStatement = getSQLParserEngine(context.getDatabaseType()).parse(sql, true);
        if (!(sqlStatement instanceof DropDatabaseStatement)) {
            return;
        }
        String databaseName = ((DropDatabaseStatement) sqlStatement).getDatabaseName();
        if (Objects.equals(context.getCurrentDatabaseName(), databaseName)) {
            context.setCurrentDatabaseName(context.getDefaultDatabaseName());
        }
        try (Connection connection = context.getActualDataSource().getConnection()) {
            try (Statement statement = connection.createStatement()) {
                statement.execute("drop database " + databaseName);
            }
        }
    }
}
