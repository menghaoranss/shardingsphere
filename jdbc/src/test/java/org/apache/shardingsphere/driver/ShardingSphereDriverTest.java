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

package org.apache.shardingsphere.driver;

import org.apache.shardingsphere.driver.jdbc.core.connection.ShardingSphereConnection;
import org.apache.shardingsphere.encrypt.exception.metadata.MissingMatchedEncryptQueryAlgorithmException;
import org.apache.shardingsphere.infra.hint.HintManager;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.ServiceLoader;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ShardingSphereDriverTest {
    
    @Test
    void assertJavaSqlDriverRegistered() {
        assertTrue(isShardingSphereDriverSPIExisting(), "Could not load ShardingSphereDriver from META-INF/services/java.sql.Driver");
    }
    
    private boolean isShardingSphereDriverSPIExisting() {
        for (Driver each : ServiceLoader.load(Driver.class)) {
            if (each instanceof ShardingSphereDriver) {
                return true;
            }
        }
        return false;
    }
    
    @Test
    void assertConnectWithInvalidURL() {
        assertThrows(SQLException.class, () -> DriverManager.getConnection("jdbc:invalid:xxx"));
    }
    
    @Test
    void assertDriverWorks() throws SQLException {
        try (
                Connection connection = DriverManager.getConnection("jdbc:shardingsphere:classpath:config/driver/foo-driver-fixture.yaml");
                Statement statement = connection.createStatement()) {
            assertThat(connection, instanceOf(ShardingSphereConnection.class));
            statement.execute("DROP TABLE IF EXISTS t_order");
            statement.execute("CREATE TABLE t_order (order_id INT PRIMARY KEY, user_id INT)");
            statement.execute("INSERT INTO t_order (order_id, user_id) VALUES (1, 101), (2, 102)");
            try (ResultSet resultSet = statement.executeQuery("SELECT COUNT(1) FROM t_order")) {
                assertTrue(resultSet.next());
                assertThat(resultSet.getInt(1), is(2));
            }
        }
    }
    
    @Test
    void assertVarbinaryColumnWorks() throws SQLException {
        try (
                Connection connection = DriverManager.getConnection("jdbc:shardingsphere:classpath:config/driver/foo-driver-fixture.yaml");
                Statement statement = connection.createStatement()) {
            assertThat(connection, instanceOf(ShardingSphereConnection.class));
            statement.execute("DROP TABLE IF EXISTS t_order");
            statement.execute("CREATE TABLE t_order (order_id VARBINARY(64) PRIMARY KEY, user_id INT)");
            PreparedStatement preparedStatement = connection.prepareStatement("INSERT INTO t_order (order_id, user_id) VALUES (?, ?)");
            preparedStatement.setBytes(1, new byte[]{-1, 0, 1});
            preparedStatement.setInt(2, 101);
            int updatedCount = preparedStatement.executeUpdate();
            assertThat(updatedCount, is(1));
            try (ResultSet resultSet = statement.executeQuery("SELECT COUNT(1) FROM t_order")) {
                assertTrue(resultSet.next());
                assertThat(resultSet.getInt(1), is(1));
            }
        }
    }
    
    @Test
    void assertDatabaseNameTransparentWithHintManager() throws SQLException {
        try (
                Connection connection = DriverManager.getConnection("jdbc:shardingsphere:classpath:config/driver/foo-driver-fixture.yaml");
                Statement statement = connection.createStatement()) {
            assertThat(connection, instanceOf(ShardingSphereConnection.class));
            statement.execute("DROP TABLE IF EXISTS t_order");
            statement.execute("CREATE TABLE t_order (order_id INT PRIMARY KEY, user_id INT)");
            statement.execute("INSERT INTO t_order (order_id, user_id) VALUES (1, 101), (2, 102)");
            try (HintManager hintManager = HintManager.getInstance()) {
                executeQueryWithHintManager(hintManager, statement);
            }
        }
    }
    
    private void executeQueryWithHintManager(final HintManager hintManager, final Statement statement) throws SQLException {
        hintManager.setDataSourceName("ds_0");
        try (ResultSet resultSet = statement.executeQuery("SELECT COUNT(1) FROM t_order_0")) {
            assertTrue(resultSet.next());
            assertThat(resultSet.getInt(1), is(1));
        }
    }
    
    @Test
    void assertExecuteQueryWhenConfigEncryptFailOver() throws SQLException {
        try (
                Connection connection = DriverManager.getConnection("jdbc:shardingsphere:classpath:config/driver/sphereex-encrypt-driver-fixture.yaml");
                Statement statement = connection.createStatement()) {
            assertThat(connection, instanceOf(ShardingSphereConnection.class));
            statement.execute("DROP TABLE IF EXISTS t_user");
            statement.execute("CREATE TABLE t_user (user_id INT PRIMARY KEY, user_name VARCHAR(50) NOT NULL, password VARCHAR(50) "
                    + "NOT NULL, email VARCHAR(50) NOT NULL, telephone VARCHAR(50) NOT NULL, creation_date DATE NOT NULL)");
            statement.execute("INSERT INTO t_user (user_id, user_name, password, email, telephone, creation_date) VALUES (1, 'zhangsan', '123', 'zhangsan@gmail.com', '12345678901', NOW())");
            assertStatementExecuteQueryResultSet(statement, "SELECT user_id, SUBSTRING(user_name, 3), password FROM t_user ORDER BY user_name",
                    Collections.singletonList(Arrays.asList(1, "angsan", "123")));
            assertPreparedStatementExecuteQueryResultSet(connection, "SELECT user_id, SUBSTRING(user_name, 3), password FROM t_user ORDER BY user_name",
                    Collections.singletonList(Arrays.asList(1, "angsan", "123")));
        }
    }
    
    private void assertStatementExecuteQueryResultSet(final Statement statement, final String sql, final Collection<Collection<Object>> expected) throws SQLException {
        try (ResultSet resultSet = statement.executeQuery(sql)) {
            assertExecuteQueryResultSet(resultSet, expected);
        }
        statement.execute(sql);
        try (ResultSet resultSet = statement.getResultSet()) {
            assertExecuteQueryResultSet(resultSet, expected);
        }
    }
    
    private void assertPreparedStatementExecuteQueryResultSet(final Connection connection, final String sql, final Collection<Collection<Object>> expected) throws SQLException {
        try (PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                assertExecuteQueryResultSet(resultSet, expected);
            }
            preparedStatement.execute();
            try (ResultSet resultSet = preparedStatement.getResultSet()) {
                assertExecuteQueryResultSet(resultSet, expected);
            }
        }
    }
    
    private void assertExecuteQueryResultSet(final ResultSet resultSet, final Collection<Collection<Object>> expected) throws SQLException {
        Iterator<Collection<Object>> rowsIterator = expected.iterator();
        int columnCount = resultSet.getMetaData().getColumnCount();
        while (rowsIterator.hasNext()) {
            assertTrue(resultSet.next());
            Object[] values = rowsIterator.next().toArray();
            for (int columnIndex = 1; columnIndex <= columnCount; columnIndex++) {
                assertThat(resultSet.getObject(columnIndex), is(values[columnIndex - 1]));
            }
        }
        assertFalse(resultSet.next());
    }
    
    @Test
    void assertExecuteQueryWhenConfigEncryptFailOverAndButAllCipherColumnConfigPlain() throws SQLException {
        try (
                Connection connection = DriverManager.getConnection("jdbc:shardingsphere:classpath:config/driver/sphereex-encrypt-driver-fixture.yaml");
                Statement statement = connection.createStatement()) {
            assertThat(connection, instanceOf(ShardingSphereConnection.class));
            statement.execute("DROP TABLE IF EXISTS t_merchant");
            statement.execute("CREATE TABLE t_merchant (merchant_id INT PRIMARY KEY, country_id INT NOT NULL, merchant_name VARCHAR(50) NOT NULL, "
                    + "business_code VARCHAR(50) NOT NULL, telephone VARCHAR(50) NOT NULL, creation_date DATE NOT NULL)");
            statement.execute("INSERT INTO t_merchant (merchant_id, country_id, merchant_name, business_code, telephone, creation_date) VALUES (1, 86, 'tencent', '86000001', '86100000001', NOW())");
            assertThrows(SQLException.class, () -> connection.prepareStatement("SELECT merchant_id, business_code, telephone FROM t_merchant ORDER BY telephone").executeQuery());
            assertThrows(SQLException.class, () -> connection.prepareStatement("SELECT merchant_id, business_code, telephone FROM t_merchant ORDER BY telephone").execute());
            assertThrows(SQLException.class, () -> statement.executeQuery("SELECT merchant_id, business_code, telephone FROM t_merchant ORDER BY telephone"));
            assertThrows(MissingMatchedEncryptQueryAlgorithmException.class, () -> statement.execute("SELECT merchant_id, business_code, telephone FROM t_merchant ORDER BY telephone"));
        }
    }
}
