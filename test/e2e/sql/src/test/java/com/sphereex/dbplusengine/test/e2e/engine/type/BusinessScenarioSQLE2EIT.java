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

package com.sphereex.dbplusengine.test.e2e.engine.type;

import com.google.common.base.Joiner;
import com.sphereex.dbplusengine.test.e2e.engine.arg.BusinessScenarioTestCaseArgumentsProvider;
import lombok.Setter;
import lombok.SneakyThrows;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.shardingsphere.test.e2e.env.E2EEnvironmentAware;
import org.apache.shardingsphere.test.e2e.env.E2EEnvironmentEngine;
import org.apache.shardingsphere.test.e2e.env.runtime.E2ETestEnvironment;
import org.apache.shardingsphere.test.e2e.framework.param.array.E2ETestParameterFactory;
import org.apache.shardingsphere.test.e2e.framework.param.model.CaseTestParameter;
import org.h2.util.ScriptReader;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.junit.jupiter.api.condition.EnabledIf;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;

import java.io.IOException;
import java.io.StringReader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

@TestInstance(Lifecycle.PER_CLASS)
@Setter
class BusinessScenarioSQLE2EIT implements E2EEnvironmentAware {
    
    private static CSVPrinter csvHeaderPrinter;
    
    private E2EEnvironmentEngine environmentEngine;
    
    @SneakyThrows(IOException.class)
    @BeforeAll
    static void beforeAll() {
        csvHeaderPrinter = new CSVPrinter(Files.newBufferedWriter(Paths.get("/tmp", Joiner.on("_").join(BusinessScenarioSQLE2EIT.class.getSimpleName(), System.currentTimeMillis(),
                E2ETestEnvironment.getInstance().getScenarios().iterator().next(), "Result.csv"))), CSVFormat.DEFAULT);
        csvHeaderPrinter.printRecord("sql", "exception", "message");
    }
    
    @SneakyThrows(IOException.class)
    @AfterAll
    static void afterAll() {
        if (null != csvHeaderPrinter) {
            csvHeaderPrinter.close(true);
        }
    }
    
    @ParameterizedTest(name = "{0}")
    @EnabledIf("isEnabled")
    @ArgumentsSource(BusinessScenarioTestCaseArgumentsProvider.class)
    void assertExecuteBatch(final CaseTestParameter testParam) throws IOException, SQLException {
        if (null == testParam.getTestCaseContext()) {
            return;
        }
        try {
            assertExecuteWithExpectedDataSource(testParam);
            // CHECKSTYLE:OFF
        } catch (Throwable t) {
            // CHECKSTYLE:ON
            csvHeaderPrinter.printRecord(testParam.getTestCaseContext().getTestCase().getSql(), t.getClass().getSimpleName(), ExceptionUtils.getStackTrace(t));
            throw t;
        }
    }
    
    private void assertExecuteWithExpectedDataSource(final CaseTestParameter testParam) throws SQLException {
        try (
                Connection targetConnection = environmentEngine.getTargetDataSource().getConnection();
                Connection expectedConnection = environmentEngine.getExpectedDataSourceMap().values().iterator().next().getConnection()) {
            assertExecuteWithMultiSql(targetConnection, expectedConnection, testParam);
        }
    }
    
    private void assertExecuteWithMultiSql(final Connection targetConnection, final Connection expectedConnection,
                                           final CaseTestParameter testParam) throws SQLException {
        String multiSql = testParam.getTestCaseContext().getTestCase().getSql();
        ScriptReader scriptReader = new ScriptReader(new StringReader(multiSql));
        while (true) {
            String sql = scriptReader.readStatement();
            if (null == sql) {
                break;
            }
            assertExecuteForStatement(targetConnection, expectedConnection, testParam, sql);
        }
    }
    
    private void assertExecuteForStatement(final Connection targetConnection, final Connection expectedConnection,
                                           final CaseTestParameter testParam, final String sql) throws SQLException {
        try (
                Statement targetStatement = targetConnection.createStatement();
                Statement expectedStatement = expectedConnection.createStatement()) {
            boolean actualResultExists = targetStatement.execute(sql);
            boolean expectedResultExists = expectedStatement.execute(getExpectedSQL(sql));
            if (isSkipAssertResult(sql)) {
                return;
            }
            assertThat("Query result mismatch.", actualResultExists, is(expectedResultExists));
            if (!actualResultExists) {
                return;
            }
            try (
                    ResultSet actualResultSet = targetStatement.getResultSet();
                    ResultSet expectedResultSet = expectedStatement.getResultSet()) {
                assertResultSet(actualResultSet, expectedResultSet, testParam);
            }
        }
    }
    
    private String getExpectedSQL(final String sql) {
        String result = sql;
        List<String> reverseSortedDatabaseNames = environmentEngine.getActualDataSourceMap().keySet().stream().sorted(Comparator.comparingInt(String::length).reversed()).collect(Collectors.toList());
        for (String each : reverseSortedDatabaseNames) {
            if (!result.contains("expected_" + each)) {
                result = result.replace(each, "expected_" + each);
            }
        }
        return result.replace("expected_dff_expected_seata", "expected_dff_seata");
    }
    
    private boolean isSkipAssertResult(final String sql) {
        String trimmedUpperCaseSQL = sql.toUpperCase().trim();
        return !trimmedUpperCaseSQL.startsWith("SELECT") && !trimmedUpperCaseSQL.startsWith("WITH") && !trimmedUpperCaseSQL.startsWith("USE");
    }
    
    protected final void assertResultSet(final ResultSet actualResultSet, final ResultSet expectedResultSet, final CaseTestParameter testParam) throws SQLException {
        assertMetaData(actualResultSet.getMetaData(), expectedResultSet.getMetaData(), testParam);
    }
    
    private void assertMetaData(final ResultSetMetaData actualResultSetMetaData, final ResultSetMetaData expectedResultSetMetaData, final CaseTestParameter testParam) throws SQLException {
        assertThat(actualResultSetMetaData.getColumnCount(), is(expectedResultSetMetaData.getColumnCount()));
        for (int i = 0; i < actualResultSetMetaData.getColumnCount(); i++) {
            assertThat(actualResultSetMetaData.getColumnLabel(i + 1), is(expectedResultSetMetaData.getColumnLabel(i + 1)));
            // todo 大小写也要一致
            // assertThat(actualResultSetMetaData.getColumnName(i + 1).toLowerCase(), is(expectedResultSetMetaData.getColumnName(i + 1).toLowerCase()));
            if ("jdbc".equals(testParam.getAdapter()) && "Cluster".equals(testParam.getMode()) && "encrypt".equals(testParam.getScenario())) {
                // FIXME correct columnType with proxy adapter and other jdbc scenario
                assertThat(actualResultSetMetaData.getColumnType(i + 1), is(expectedResultSetMetaData.getColumnType(i + 1)));
            }
        }
    }
    
    private static boolean isEnabled() {
        return E2ETestParameterFactory.containsTestParameter();
    }
}
