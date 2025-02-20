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

package com.sphereex.dbplusengine.test.e2e.framework.database.impl;

import com.sphereex.dbplusengine.SphereEx;
import org.apache.shardingsphere.test.e2e.framework.database.DatabaseAssertionMetaData;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * DM assertion meta data.
 */
@SphereEx
public final class DMDatabaseAssertionMetaData implements DatabaseAssertionMetaData {
    
    @Override
    public String getPrimaryKeyColumnName(final DataSource dataSource, final String tableName) throws SQLException {
        String sql = String.format("SELECT A.COLUMN_NAME FROM ALL_CONS_COLUMNS A where A.TABLE_NAME = '%s'", tableName.toUpperCase());
        try (
                Connection connection = dataSource.getConnection();
                Statement statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery(sql)) {
            if (resultSet.next()) {
                return resultSet.getString("COLUMN_NAME");
            }
            throw new SQLException(String.format("Can not get primary key of `%s`", tableName));
        }
    }
}
