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

package com.sphereex.dbplusengine.test.e2e.engine.type.plsql;

import com.sphereex.dbplusengine.test.e2e.engine.composer.hybrid.HybridE2EContainerComposer;
import com.sphereex.dbplusengine.test.e2e.engine.composer.hybrid.HybridE2EContainerComposerRegistry;
import org.apache.shardingsphere.infra.datanode.DataNode;
import org.apache.shardingsphere.infra.expr.core.InlineExpressionParserFactory;
import org.apache.shardingsphere.test.e2e.cases.casse.assertion.E2ETestCaseAssertion;
import org.apache.shardingsphere.test.e2e.cases.dataset.DataSet;
import org.apache.shardingsphere.test.e2e.cases.dataset.DataSetLoader;
import org.apache.shardingsphere.test.e2e.cases.dataset.metadata.DataSetColumn;
import org.apache.shardingsphere.test.e2e.cases.dataset.metadata.DataSetMetaData;
import org.apache.shardingsphere.test.e2e.cases.dataset.row.DataSetRow;
import org.apache.shardingsphere.test.e2e.cases.value.SQLValue;
import org.apache.shardingsphere.test.e2e.engine.arg.E2ETestCaseArgumentsProvider;
import org.apache.shardingsphere.test.e2e.engine.arg.E2ETestCaseSettings;
import org.apache.shardingsphere.test.e2e.env.DataSetEnvironmentManager;
import org.apache.shardingsphere.test.e2e.env.runtime.scenario.path.ScenarioDataPath;
import org.apache.shardingsphere.test.e2e.env.runtime.scenario.path.ScenarioDataPath.Type;
import org.apache.shardingsphere.test.e2e.framework.database.DatabaseAssertionMetaData;
import org.apache.shardingsphere.test.e2e.framework.database.DatabaseAssertionMetaDataFactory;
import org.apache.shardingsphere.test.e2e.framework.param.array.E2ETestParameterFactory;
import org.apache.shardingsphere.test.e2e.framework.param.model.AssertionTestParameter;
import org.apache.shardingsphere.test.e2e.framework.type.SQLCommandType;
import org.apache.shardingsphere.test.e2e.framework.type.SQLExecuteType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.condition.EnabledIf;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;

import javax.sql.DataSource;
import javax.xml.bind.JAXBException;
import java.io.IOException;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

@E2ETestCaseSettings(SQLCommandType.PL_SQL)
class PlSqlE2EIT {
    
    private static final String DATA_COLUMN_DELIMITER = ", ";
    
    private DataSetEnvironmentManager dataSetEnvironmentManager;
    
    private AssertionTestParameter testParam;
    
    @ParameterizedTest(name = "{0}")
    @EnabledIf("isEnabled")
    @ArgumentsSource(PlSqlE2ETestCaseArgumentsProvider.class)
    void assertExecute(final AssertionTestParameter testParam) throws SQLException, JAXBException, IOException {
        if (null == testParam.getDatabaseType() || !"Oracle".equalsIgnoreCase(testParam.getDatabaseType().getType())) {
            return;
        }
        assumeTrue(null != testParam.getTestCaseContext());
        HybridE2EContainerComposer containerComposer = HybridE2EContainerComposerRegistry.getInstance().getContainerComposer(testParam.getScenario(), testParam.getDatabaseType(), testParam.getMode());
        init(testParam, containerComposer);
        try (
                Connection connection = containerComposer.getDriverDataSource().getConnection();
                CallableStatement callableStatement = prepareCall(connection, testParam)) {
            assertFalse(callableStatement.execute());
        }
        assertDataSet(testParam, containerComposer);
    }
    
    private void init(final AssertionTestParameter testParam, final HybridE2EContainerComposer containerComposer) throws IOException, JAXBException {
        dataSetEnvironmentManager = new DataSetEnvironmentManager(
                new ScenarioDataPath(testParam.getScenario()).getDataSetFile(Type.ACTUAL, testParam.getDatabaseType()), containerComposer.getActualDataSourceMap(), testParam.getDatabaseType());
        dataSetEnvironmentManager.fillData();
        this.testParam = testParam;
    }
    
    private CallableStatement prepareCall(final Connection connection, final AssertionTestParameter testParam) throws SQLException {
        CallableStatement result = connection.prepareCall(decorateSQL(testParam));
        if (SQLExecuteType.PLACEHOLDER == testParam.getSqlExecuteType()) {
            for (SQLValue each : testParam.getAssertion().getSQLValues()) {
                result.setObject(each.getIndex(), each.getValue());
            }
        }
        return result;
    }
    
    private String decorateSQL(final AssertionTestParameter testParam) {
        String sql = testParam.getTestCaseContext().getTestCase().getSql();
        return testParam.getSqlExecuteType() == SQLExecuteType.LITERAL ? getLiteralSQL(testParam.getAssertion(), sql) : sql;
    }
    
    private String getLiteralSQL(final E2ETestCaseAssertion assertion, final String sql) {
        List<Object> params = null == assertion ? Collections.emptyList() : assertion.getSQLValues().stream().map(SQLValue::toString).collect(Collectors.toList());
        return params.isEmpty() ? sql : String.format(sql.replace("%", "ÿ").replace("?", "%s"), params.toArray()).replace("ÿ", "%").replace("%%", "%").replace("'%'", "'%%'");
    }
    
    private void assertDataSet(final AssertionTestParameter testParam, final HybridE2EContainerComposer containerComposer) throws SQLException {
        // TODO Support asserting multi tables data set
        E2ETestCaseAssertion assertion = testParam.getAssertion();
        DataSet dataSet = null == assertion || null == assertion.getExpectedDataFile() ? null
                : DataSetLoader.load(testParam.getTestCaseContext().getParentPath(), testParam.getScenario(), testParam.getDatabaseType(), testParam.getMode(), assertion.getExpectedDataFile());
        assertThat("Only support single table for PL/SQL for now.", dataSet.getMetaDataList().size(), is(1));
        DataSetMetaData expectedDataSetMetaData = dataSet.getMetaDataList().get(0);
        for (String each : InlineExpressionParserFactory.newInstance(expectedDataSetMetaData.getDataNodes()).splitAndEvaluate()) {
            DataNode dataNode = new DataNode(each);
            DataSource dataSource = containerComposer.getActualDataSourceMap().get(dataNode.getDataSourceName());
            try (
                    Connection connection = dataSource.getConnection();
                    PreparedStatement preparedStatement = connection.prepareStatement(generateFetchActualDataSQL(testParam, containerComposer, dataNode))) {
                assertDataSet(preparedStatement, expectedDataSetMetaData, dataSet.findRows(dataNode));
            }
        }
    }
    
    private void assertDataSet(final PreparedStatement actualPreparedStatement, final DataSetMetaData expectedDataSetMetaData, final List<DataSetRow> expectedDataSetRows) throws SQLException {
        try (ResultSet actualResultSet = actualPreparedStatement.executeQuery()) {
            assertMetaData(actualResultSet.getMetaData(), expectedDataSetMetaData.getColumns());
            assertRows(actualResultSet, expectedDataSetRows);
        }
    }
    
    private String generateFetchActualDataSQL(final AssertionTestParameter testParam, final HybridE2EContainerComposer containerComposer, final DataNode dataNode) throws SQLException {
        Optional<DatabaseAssertionMetaData> databaseAssertionMetaData = DatabaseAssertionMetaDataFactory.newInstance(testParam.getDatabaseType());
        if (databaseAssertionMetaData.isPresent()) {
            String primaryKeyColumnName = databaseAssertionMetaData.get().getPrimaryKeyColumnName(
                    containerComposer.getActualDataSourceMap().get(dataNode.getDataSourceName()), dataNode.getTableName());
            return String.format("SELECT * FROM %s ORDER BY %s ASC", dataNode.getTableName(), primaryKeyColumnName);
        }
        return String.format("SELECT * FROM %s", dataNode.getTableName());
    }
    
    private void assertMetaData(final ResultSetMetaData actual, final Collection<DataSetColumn> expected) throws SQLException {
        assertThat(actual.getColumnCount(), is(expected.size()));
        int index = 1;
        for (DataSetColumn each : expected) {
            assertThat(actual.getColumnLabel(index++).toUpperCase(), is(each.getName().toUpperCase()));
        }
    }
    
    private void assertRows(final ResultSet actual, final List<DataSetRow> expected) throws SQLException {
        int rowCount = 0;
        while (actual.next()) {
            int columnIndex = 1;
            for (String each : expected.get(rowCount).splitValues(DATA_COLUMN_DELIMITER)) {
                assertThat(String.valueOf(actual.getObject(columnIndex)), is(each));
                columnIndex++;
            }
            rowCount++;
        }
        assertThat("Size of actual result set is different with size of expected dat set rows.", rowCount, is(expected.size()));
    }
    
    @AfterEach
    void tearDown() {
        // TODO make sure test case can not be null
        if (null != dataSetEnvironmentManager) {
            dataSetEnvironmentManager.cleanData();
        }
        if (null != testParam) {
            HybridE2EContainerComposerRegistry.getInstance().release(testParam.getScenario(), testParam.getDatabaseType(), testParam.getMode());
        }
    }
    
    private static boolean isEnabled() {
        return E2ETestParameterFactory.containsTestParameter();
    }
    
    private static class PlSqlE2ETestCaseArgumentsProvider implements ArgumentsProvider {
        
        @Override
        public Stream<? extends Arguments> provideArguments(final ExtensionContext context) {
            return new E2ETestCaseArgumentsProvider().provideArguments(context).peek(each -> {
                AssertionTestParameter testParam = (AssertionTestParameter) each.get()[0];
                HybridE2EContainerComposerRegistry.getInstance().retain(testParam.getScenario(), testParam.getDatabaseType(), testParam.getMode());
            }).collect(Collectors.toList()).stream();
        }
    }
}
