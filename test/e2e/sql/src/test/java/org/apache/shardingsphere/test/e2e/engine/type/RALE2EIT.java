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

package org.apache.shardingsphere.test.e2e.engine.type;

import com.google.common.base.Splitter;
import com.sphereex.dbplusengine.SphereEx;
import org.apache.shardingsphere.test.e2e.framework.type.SQLCommandType;
import org.apache.shardingsphere.test.e2e.cases.dataset.metadata.DataSetColumn;
import org.apache.shardingsphere.test.e2e.cases.dataset.metadata.DataSetMetaData;
import org.apache.shardingsphere.test.e2e.cases.dataset.row.DataSetRow;
import org.apache.shardingsphere.test.e2e.env.E2EEnvironmentAware;
import org.apache.shardingsphere.test.e2e.env.E2EEnvironmentEngine;
import org.apache.shardingsphere.test.e2e.engine.arg.E2ETestCaseArgumentsProvider;
import org.apache.shardingsphere.test.e2e.engine.arg.E2ETestCaseSettings;
import org.apache.shardingsphere.test.e2e.engine.context.E2ETestContext;
import org.apache.shardingsphere.test.e2e.framework.param.array.E2ETestParameterFactory;
import org.apache.shardingsphere.test.e2e.framework.param.model.AssertionTestParameter;
import org.junit.jupiter.api.condition.EnabledIf;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;
import org.testcontainers.shaded.org.awaitility.Awaitility;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;

@E2ETestCaseSettings(SQLCommandType.RAL)
class RALE2EIT implements E2EEnvironmentAware {
    
    @SphereEx
    private static final Pattern SQL_FEDERATION_SHUFFLE_JOIN_TEMP_TABLE_PATTERN = Pattern.compile("_[0-9a-zA-Z]{32}");
    
    @SphereEx
    private static final String SPEX_VERTICAL_LINE = "{SPEX_VERTICAL_LINE}";
    
    private E2EEnvironmentEngine environmentEngine;
    
    @Override
    public void setEnvironmentEngine(final E2EEnvironmentEngine environmentEngine) {
        this.environmentEngine = environmentEngine;
    }
    
    @ParameterizedTest(name = "{0}")
    @EnabledIf("isEnabled")
    @ArgumentsSource(E2ETestCaseArgumentsProvider.class)
    void assertExecute(final AssertionTestParameter testParam) throws SQLException {
        // TODO make sure test case can not be null
        if (null == testParam.getTestCaseContext()) {
            return;
        }
        E2ETestContext context = new E2ETestContext(testParam);
        init(context);
        try {
            assertExecute(context);
        } finally {
            tearDown(context);
        }
    }
    
    private void assertExecute(final E2ETestContext context) throws SQLException {
        try (Connection connection = environmentEngine.getTargetDataSource().getConnection()) {
            try (Statement statement = connection.createStatement()) {
                assertResultSet(context, statement);
            }
        }
    }
    
    private void init(final E2ETestContext context) throws SQLException {
        if (null != context.getAssertion().getInitialSQL()) {
            try (Connection connection = environmentEngine.getTargetDataSource().getConnection()) {
                executeInitSQLs(context, connection);
            }
        }
    }
    
    private void executeInitSQLs(final E2ETestContext context, final Connection connection) throws SQLException {
        if (null == context.getAssertion().getInitialSQL().getSql()) {
            return;
        }
        for (String each : Splitter.on(";").trimResults().omitEmptyStrings().splitToList(context.getAssertion().getInitialSQL().getSql())) {
            try (PreparedStatement preparedStatement = connection.prepareStatement(each)) {
                preparedStatement.executeUpdate();
            }
            // SPEX ADDED: BEGIN
            Awaitility.await().pollDelay(1L, TimeUnit.SECONDS).until(() -> true);
            // SPEX ADDED: END
        }
        Awaitility.await().pollDelay(1L, TimeUnit.SECONDS).until(() -> true);
    }
    
    private void tearDown(final E2ETestContext context) throws SQLException {
        if (null != context.getAssertion().getDestroySQL()) {
            try (Connection connection = environmentEngine.getTargetDataSource().getConnection()) {
                executeDestroySQLs(context, connection);
            }
        }
    }
    
    private void executeDestroySQLs(final E2ETestContext context, final Connection connection) throws SQLException {
        if (null == context.getAssertion().getDestroySQL().getSql()) {
            return;
        }
        for (String each : Splitter.on(";").trimResults().omitEmptyStrings().splitToList(context.getAssertion().getDestroySQL().getSql())) {
            try (PreparedStatement preparedStatement = connection.prepareStatement(each)) {
                preparedStatement.executeUpdate();
            }
            // SPEX ADDED: BEGIN
            Awaitility.await().pollDelay(1L, TimeUnit.SECONDS).until(() -> true);
            // SPEX ADDED: END
        }
        Awaitility.await().pollDelay(1L, TimeUnit.SECONDS).until(() -> true);
    }
    
    private void assertResultSet(final E2ETestContext context, final Statement statement) throws SQLException {
        if (null == context.getAssertion().getAssertionSQL()) {
            assertResultSet(context, statement, context.getSQL());
        } else {
            statement.execute(context.getSQL());
            Awaitility.await().pollDelay(2L, TimeUnit.SECONDS).until(() -> true);
            assertResultSet(context, statement, context.getAssertion().getAssertionSQL().getSql());
        }
    }
    
    private void assertResultSet(final E2ETestContext context, final Statement statement, final String sql) throws SQLException {
        statement.execute(sql);
        try (ResultSet resultSet = statement.getResultSet()) {
            assertResultSet(context, resultSet);
        }
    }
    
    private void assertResultSet(final E2ETestContext context, final ResultSet resultSet) throws SQLException {
        assertMetaData(resultSet.getMetaData(), getExpectedColumns(context));
        // SPEX CHANGED: BEGIN
        assertRows(resultSet, getIgnoreAssertColumns(context), context.getDataSet().getRows(), context.getScenario());
        // SPEX CHANGED: END
    }
    
    private Collection<DataSetColumn> getExpectedColumns(final E2ETestContext context) {
        Collection<DataSetColumn> result = new LinkedList<>();
        for (DataSetMetaData each : context.getDataSet().getMetaDataList()) {
            result.addAll(each.getColumns());
        }
        return result;
    }
    
    private Collection<String> getIgnoreAssertColumns(final E2ETestContext context) {
        Collection<String> result = new LinkedList<>();
        for (DataSetMetaData each : context.getDataSet().getMetaDataList()) {
            result.addAll(each.getColumns().stream().filter(DataSetColumn::isIgnoreAssertData).map(DataSetColumn::getName).collect(Collectors.toList()));
        }
        return result;
    }
    
    private void assertMetaData(final ResultSetMetaData actual, final Collection<DataSetColumn> expected) throws SQLException {
        assertThat(actual.getColumnCount(), is(expected.size()));
        int index = 1;
        for (DataSetColumn each : expected) {
            assertThat(actual.getColumnLabel(index++).toLowerCase(), is(each.getName().toLowerCase()));
        }
    }
    
    private void assertRows(final ResultSet actual, final Collection<String> notAssertionColumns, final List<DataSetRow> expected, @SphereEx final String scenario) throws SQLException {
        int rowCount = 0;
        ResultSetMetaData actualMetaData = actual.getMetaData();
        while (actual.next()) {
            assertTrue(rowCount < expected.size(), "Size of actual result set is different with size of expected data set rows.");
            // SPEX CHANGED: BEGIN
            assertRow(actual, notAssertionColumns, actualMetaData, expected.get(rowCount), scenario);
            // SPEX CHANGED: END
            rowCount++;
        }
        assertThat("Size of actual result set is different with size of expected data set rows.", rowCount, is(expected.size()));
    }
    
    private void assertRow(final ResultSet actual, final Collection<String> notAssertionColumns, final ResultSetMetaData actualMetaData, final DataSetRow expected,
                           @SphereEx final String scenario) throws SQLException {
        int columnIndex = 1;
        for (String each : expected.splitValues("|")) {
            String columnLabel = actualMetaData.getColumnLabel(columnIndex);
            if (!notAssertionColumns.contains(columnLabel)) {
                // SPEX CHANGED: BEGIN
                assertObjectValue(actual, columnIndex, columnLabel, each, scenario);
                // SPEX CHANGED: END
            }
            columnIndex++;
        }
    }
    
    private void assertObjectValue(final ResultSet actual, final int columnIndex, final String columnLabel, final String expected, @SphereEx final String scenario) throws SQLException {
        // SPEX ADDED: BEGIN
        if ("sphereex_sql_federation_shuffle_join".equals(scenario)) {
            String replacedExpected = SQL_FEDERATION_SHUFFLE_JOIN_TEMP_TABLE_PATTERN.matcher(expected).replaceAll("").replace(SPEX_VERTICAL_LINE, "|");
            assertThat(SQL_FEDERATION_SHUFFLE_JOIN_TEMP_TABLE_PATTERN.matcher(String.valueOf(actual.getObject(columnIndex)).trim()).replaceAll(""), is(replacedExpected));
            assertThat(SQL_FEDERATION_SHUFFLE_JOIN_TEMP_TABLE_PATTERN.matcher(String.valueOf(actual.getObject(columnLabel)).trim()).replaceAll(""), is(replacedExpected));
            return;
        }
        // SPEX ADDED: END
        assertThat(String.valueOf(actual.getObject(columnIndex)).trim(), is(expected));
        assertThat(String.valueOf(actual.getObject(columnLabel)).trim(), is(expected));
    }
    
    private static boolean isEnabled() {
        return E2ETestParameterFactory.containsTestParameter() && !E2ETestParameterFactory.getAssertionTestParameters(SQLCommandType.RAL).isEmpty();
    }
}
