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

package com.sphereex.dbplusengine.encrypt.datanode;

import org.apache.shardingsphere.infra.datanode.DataNode;
import org.apache.shardingsphere.test.fixture.jdbc.MockedDataSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class EncryptTableDataNodeLoaderTest {
    
    private static final String[] TYPES = new String[]{"TABLE", "PARTITIONED TABLE", "VIEW", "SYSTEM TABLE", "SYSTEM VIEW"};
    
    private static final String TABLE_NAME = "TABLE_NAME";
    
    private Map<String, DataSource> dataSourceMap;
    
    @BeforeEach
    void setUp() throws SQLException {
        dataSourceMap = new LinkedHashMap<>(2, 1F);
        dataSourceMap.put("ds0", mockDataSource("ds0", Arrays.asList("employee", "dept", "salary")));
        dataSourceMap.put("ds1", mockDataSource("ds1", Arrays.asList("student", "teacher", "class", "salary")));
    }
    
    private DataSource mockDataSource(final String dataSourceName, final List<String> tableNames) throws SQLException {
        Connection connection = mock(Connection.class, RETURNS_DEEP_STUBS);
        when(connection.getCatalog()).thenReturn(dataSourceName);
        ResultSet resultSet = mockResultSet(tableNames);
        when(connection.getMetaData().getTables(dataSourceName, null, null, TYPES)).thenReturn(resultSet);
        when(connection.getMetaData().getURL()).thenReturn("jdbc:mock://127.0.0.1/foo_ds");
        return new MockedDataSource(connection);
    }
    
    private ResultSet mockResultSet(final List<String> tableNames) throws SQLException {
        ResultSet result = mock(ResultSet.class);
        String firstTableName = tableNames.get(0);
        Collection<String> remainTableNames = tableNames.subList(1, tableNames.size());
        Collection<Boolean> remainNextResults = remainTableNames.stream().map(each -> true).collect(Collectors.toList());
        remainNextResults.add(false);
        when(result.next()).thenReturn(true, remainNextResults.toArray(new Boolean[tableNames.size()]));
        when(result.getString(TABLE_NAME)).thenReturn(firstTableName, remainTableNames.toArray(new String[tableNames.size() - 1]));
        return result;
    }
    
    @Test
    void assertLoad() {
        Map<String, Collection<DataNode>> actual = EncryptTableDataNodeLoader.load("foo_db", dataSourceMap, Collections.singleton("teacher"));
        assertFalse(actual.containsKey("employee"));
        assertFalse(actual.containsKey("salary"));
        assertFalse(actual.containsKey("student"));
        assertFalse(actual.containsKey("dept"));
        assertTrue(actual.containsKey("teacher"));
        assertFalse(actual.containsKey("class"));
        assertFalse(actual.containsKey("salary"));
        assertThat(actual.get("teacher").iterator().next().getDataSourceName(), is("ds1"));
    }
    
    @Test
    void assertLoadWithNotExistTable() {
        Map<String, Collection<DataNode>> actual = EncryptTableDataNodeLoader.load("foo_db", dataSourceMap, Collections.singleton("t_encrypt"));
        assertTrue(actual.isEmpty());
    }
}
