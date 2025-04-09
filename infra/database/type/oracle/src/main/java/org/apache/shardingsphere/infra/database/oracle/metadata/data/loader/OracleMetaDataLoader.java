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

package org.apache.shardingsphere.infra.database.oracle.metadata.data.loader;

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.sphereex.dbplusengine.SphereEx;
import com.sphereex.dbplusengine.SphereEx.Type;
import org.apache.shardingsphere.infra.database.core.metadata.data.loader.DialectMetaDataLoader;
import org.apache.shardingsphere.infra.database.core.metadata.data.loader.MetaDataLoaderConnection;
import org.apache.shardingsphere.infra.database.core.metadata.data.loader.MetaDataLoaderMaterial;
import org.apache.shardingsphere.infra.database.core.metadata.data.model.ColumnMetaData;
import org.apache.shardingsphere.infra.database.core.metadata.data.model.IndexMetaData;
import org.apache.shardingsphere.infra.database.core.metadata.data.model.SchemaMetaData;
import org.apache.shardingsphere.infra.database.core.metadata.data.model.TableMetaData;
import org.apache.shardingsphere.infra.database.core.metadata.database.datatype.DataTypeRegistry;
import org.apache.shardingsphere.infra.database.core.metadata.database.enums.TableType;
import org.apache.shardingsphere.infra.database.core.type.DatabaseType;
import org.apache.shardingsphere.infra.spi.type.typed.TypedSPILoader;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;

/**
 * Meta data loader for Oracle.
 */
public final class OracleMetaDataLoader implements DialectMetaDataLoader {
    
    @SphereEx(Type.MODIFY)
    private static final String TABLE_META_DATA_SQL_NO_ORDER =
            "SELECT OWNER AS TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, NULLABLE, DATA_TYPE, COLUMN_ID, HIDDEN_COLUMN %s,"
                    + " CASE"
                    + " WHEN DATA_TYPE IN ('VARCHAR2', 'CHAR') THEN"
                    + " DATA_TYPE || '(' || DATA_LENGTH || ')'"
                    + " ELSE DATA_TYPE"
                    + " END AS COLUMN_TYPE"
                    + " FROM ALL_TAB_COLS WHERE OWNER IN (%s)";
    
    private static final String ORDER_BY_COLUMN_ID = " ORDER BY COLUMN_ID";
    
    private static final String TABLE_META_DATA_SQL = TABLE_META_DATA_SQL_NO_ORDER + ORDER_BY_COLUMN_ID;
    
    private static final String TABLE_META_DATA_SQL_IN_TABLES = TABLE_META_DATA_SQL_NO_ORDER + " AND TABLE_NAME IN (%s)" + ORDER_BY_COLUMN_ID;
    
    private static final String VIEW_META_DATA_SQL = "SELECT VIEW_NAME FROM ALL_VIEWS WHERE OWNER IN (%s) AND VIEW_NAME IN (%s)";
    
    private static final String INDEX_META_DATA_SQL = "SELECT OWNER AS TABLE_SCHEMA, TABLE_NAME, INDEX_NAME, UNIQUENESS FROM ALL_INDEXES WHERE OWNER IN (%s) AND TABLE_NAME IN (%s)";
    
    private static final String PRIMARY_KEY_META_DATA_SQL = "SELECT A.OWNER AS TABLE_SCHEMA, A.TABLE_NAME AS TABLE_NAME, B.COLUMN_NAME AS COLUMN_NAME FROM ALL_CONSTRAINTS A INNER JOIN"
            + " ALL_CONS_COLUMNS B ON A.CONSTRAINT_NAME = B.CONSTRAINT_NAME WHERE CONSTRAINT_TYPE = 'P' AND A.OWNER IN (%s)";
    
    private static final String PRIMARY_KEY_META_DATA_SQL_IN_TABLES = PRIMARY_KEY_META_DATA_SQL + " AND A.TABLE_NAME IN (%s)";
    
    private static final String INDEX_COLUMN_META_DATA_SQL = "SELECT COLUMN_NAME FROM ALL_IND_COLUMNS WHERE INDEX_OWNER IN (%s) AND TABLE_NAME = ? AND INDEX_NAME = ?";
    
    @SphereEx
    private static final String USER_SYNONYMS_SQL = "SELECT * FROM USER_SYNONYMS";
    
    private static final int COLLATION_START_MAJOR_VERSION = 12;
    
    private static final int COLLATION_START_MINOR_VERSION = 2;
    
    private static final int IDENTITY_COLUMN_START_MINOR_VERSION = 1;
    
    private static final int MAX_EXPRESSION_SIZE = 1000;
    
    @SphereEx
    private static final int USER_SYNONYMS_TABLE_OWNER = 2;
    
    @SphereEx
    private static final int USER_SYNONYMS_TABLE_NAME = 3;
    
    @SphereEx
    private static final int USER_SYNONYMS_SYNONYM_NAME = 1;
    
    @Override
    public Collection<SchemaMetaData> load(final MetaDataLoaderMaterial material) throws SQLException {
        Collection<TableMetaData> tableMetaDataList = new LinkedList<>();
        try (Connection connection = new MetaDataLoaderConnection(TypedSPILoader.getService(DatabaseType.class, "Oracle"), material.getDataSource().getConnection())) {
            // SPEX ADDED: BEGIN
            tableMetaDataList.addAll(getSchemaTableNameSynonymMaps(material, connection));
            // SPEX ADDED: END
            // SPEX CHANGED: BEGIN
            tableMetaDataList.addAll(getTableMetaDataList(connection, material, connection.getSchema(), material.getActualTableNames()));
            // SPEX CHANGED: END
        }
        return Collections.singletonList(new SchemaMetaData(material.getDefaultSchemaName(), tableMetaDataList));
    }
    
    @SphereEx
    private Collection<TableMetaData> getSchemaTableNameSynonymMaps(final MetaDataLoaderMaterial material, final Connection connection) throws SQLException {
        Collection<TableMetaData> result = new LinkedList<>();
        Map<String, Map<String, String>> schemaTableNameSynonymMaps = loadLinkedTableNameSynonymMapSchemaGroup(connection);
        Collection<String> actualTableNames = new HashSet<>(material.getActualTableNames());
        for (Entry<String, Map<String, String>> entry : schemaTableNameSynonymMaps.entrySet()) {
            Collection<String> neededLinkedTableNames = entry.getValue().entrySet().stream().filter(each -> actualTableNames.contains(each.getValue())).map(Entry::getKey)
                    .collect(Collectors.toList());
            Collection<TableMetaData> linkedTableMetaDataList = getTableMetaDataList(connection, material, entry.getKey(), neededLinkedTableNames);
            result.addAll(rebuildSynonymMetaDataList(linkedTableMetaDataList, entry.getValue()));
        }
        return result;
    }
    
    @SphereEx
    private Collection<TableMetaData> rebuildSynonymMetaDataList(final Collection<TableMetaData> linkedTableMetaDataList, final Map<String, String> linkedTableNameSynonymMap) {
        Collection<TableMetaData> result = new LinkedList<>();
        for (TableMetaData each : linkedTableMetaDataList) {
            TableMetaData tableMetaData = new TableMetaData(linkedTableNameSynonymMap.get(each.getName()), each.getColumns(), each.getIndexes(), each.getConstraints(), each.getType(),
                    each.getCharacterSetName());
            result.add(tableMetaData);
        }
        return result;
    }
    
    private Collection<TableMetaData> getTableMetaDataList(final Connection connection, final MetaDataLoaderMaterial material,
                                                           final String schema, final Collection<String> tableNames) throws SQLException {
        Collection<String> viewNames = new LinkedList<>();
        Map<String, Collection<ColumnMetaData>> columnMetaDataMap = new HashMap<>(tableNames.size(), 1F);
        Map<String, Collection<IndexMetaData>> indexMetaDataMap = new HashMap<>(tableNames.size(), 1F);
        for (List<String> each : Lists.partition(new ArrayList<>(tableNames), MAX_EXPRESSION_SIZE)) {
            viewNames.addAll(loadViewNames(connection, each, generateOwner(material, schema)));
            // SPEX CHANGED: BEGIN
            columnMetaDataMap.putAll(loadColumnMetaDataMap(connection, each, generateOwner(material, schema)));
            // SPEX CHANGED: END
            indexMetaDataMap.putAll(loadIndexMetaData(connection, each, generateOwner(material, schema)));
        }
        Collection<TableMetaData> result = new LinkedList<>();
        for (Entry<String, Collection<ColumnMetaData>> entry : columnMetaDataMap.entrySet()) {
            result.add(new TableMetaData(entry.getKey(), entry.getValue(), indexMetaDataMap.getOrDefault(entry.getKey(), Collections.emptyList()), Collections.emptyList(),
                    // TODO load characterSetName here
                    // SPEX CHANGED: BEGIN
                    viewNames.contains(entry.getKey()) ? TableType.VIEW : TableType.TABLE, null));
            // SPEX CHANGED: END
        }
        return result;
    }
    
    private String generateOwner(final MetaDataLoaderMaterial material, final String userName) {
        StringBuilder result = new StringBuilder();
        result.append("'").append(userName).append("'");
        if (userName.endsWith("OPR")) {
            result.append(",'").append(userName.substring(0, userName.length() - 3) + "DATA'");
        }
        if (null != material.getProps() && material.getProps().containsKey("load-metadata-schema")) {
            result.append(",'").append((String) material.getProps().get("load-metadata-schema")).append("'");
        }
        Collection<String> schemas = Splitter.on(",").omitEmptyStrings().trimResults().splitToList(result.toString());
        if (!schemas.contains(material.getDefaultSchemaName().toUpperCase())) {
            result.append(",'").append(material.getDefaultSchemaName().toUpperCase()).append("'");
        }
        return result.toString();
    }
    
    @SphereEx
    private Map<String, Map<String, String>> loadLinkedTableNameSynonymMapSchemaGroup(final Connection connection) throws SQLException {
        Map<String, Map<String, String>> result = new HashMap<>();
        try (Statement statement = connection.createStatement()) {
            try (ResultSet resultSet = statement.executeQuery(USER_SYNONYMS_SQL)) {
                while (resultSet.next()) {
                    Map<String, String> linkedTableNameSynonymMap = result.computeIfAbsent(resultSet.getString(USER_SYNONYMS_TABLE_OWNER), unused -> new HashMap<>());
                    linkedTableNameSynonymMap.put(resultSet.getString(USER_SYNONYMS_TABLE_NAME), resultSet.getString(USER_SYNONYMS_SYNONYM_NAME));
                }
            }
        }
        return result;
    }
    
    private Collection<String> loadViewNames(final Connection connection, final Collection<String> tables, final String schema) throws SQLException {
        Collection<String> result = new LinkedList<>();
        try (PreparedStatement preparedStatement = connection.prepareStatement(getViewMetaDataSQL(tables, schema))) {
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    result.add(resultSet.getString(1));
                }
            }
        }
        return result;
    }
    
    private String getViewMetaDataSQL(final Collection<String> tableNames, final String schema) {
        return String.format(VIEW_META_DATA_SQL, schema, tableNames.stream().map(each -> String.format("'%s'", each)).collect(Collectors.joining(",")));
    }
    
    private Map<String, Collection<ColumnMetaData>> loadColumnMetaDataMap(final Connection connection, final Collection<String> tables, final String schema) throws SQLException {
        Map<String, Collection<ColumnMetaData>> result = new HashMap<>(tables.size(), 1F);
        try (PreparedStatement preparedStatement = connection.prepareStatement(getTableMetaDataSQL(tables, connection.getMetaData(), schema))) {
            Map<String, Collection<String>> tablePrimaryKeys = loadTablePrimaryKeys(connection, tables, schema);
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                // SPEX CHANGED: BEGIN
                resultSet.setFetchSize(1000);
                // SPEX CHANGED: END
                while (resultSet.next()) {
                    String tableName = resultSet.getString("TABLE_NAME");
                    ColumnMetaData columnMetaData = loadColumnMetaData(resultSet, tablePrimaryKeys.getOrDefault(tableName, Collections.emptyList()), connection.getMetaData());
                    if (!result.containsKey(tableName)) {
                        result.put(tableName, new LinkedList<>());
                    }
                    result.get(tableName).add(columnMetaData);
                }
            }
        }
        return result;
    }
    
    private ColumnMetaData loadColumnMetaData(final ResultSet resultSet, final Collection<String> primaryKeys, final DatabaseMetaData databaseMetaData) throws SQLException {
        String columnName = resultSet.getString("COLUMN_NAME");
        String dataType = getOriginalDataType(resultSet.getString("DATA_TYPE"));
        boolean primaryKey = primaryKeys.contains(columnName);
        boolean generated = versionContainsIdentityColumn(databaseMetaData) && "YES".equals(resultSet.getString("IDENTITY_COLUMN"));
        // TODO need to support caseSensitive when version < 12.2.
        String collation = versionContainsCollation(databaseMetaData) ? resultSet.getString("COLLATION") : null;
        boolean caseSensitive = null != collation && collation.endsWith("_CS");
        boolean isVisible = "NO".equals(resultSet.getString("HIDDEN_COLUMN"));
        boolean nullable = "Y".equals(resultSet.getString("NULLABLE"));
        @SphereEx
        String dataTypeContent = resultSet.getString("COLUMN_TYPE");
        // SPEX CHANGED: BEGIN
        return new ColumnMetaData(columnName, DataTypeRegistry.getDataType(getDatabaseType(), dataType).orElse(Types.OTHER), primaryKey, generated, caseSensitive, isVisible, false, nullable,
                dataTypeContent);
        // SPEX CHANGED: END
    }
    
    private String getOriginalDataType(final String dataType) {
        int index = dataType.indexOf('(');
        if (index > 0) {
            return dataType.substring(0, index);
        }
        return dataType;
    }
    
    private String getTableMetaDataSQL(final Collection<String> tables, final DatabaseMetaData databaseMetaData, final String schema) throws SQLException {
        StringBuilder stringBuilder = new StringBuilder(28);
        if (versionContainsIdentityColumn(databaseMetaData)) {
            stringBuilder.append(", IDENTITY_COLUMN");
        }
        if (versionContainsCollation(databaseMetaData)) {
            stringBuilder.append(", COLLATION");
        }
        String collation = stringBuilder.toString();
        return tables.isEmpty() ? String.format(TABLE_META_DATA_SQL, collation, schema)
                : String.format(TABLE_META_DATA_SQL_IN_TABLES, collation, schema, tables.stream().map(each -> String.format("'%s'", each)).collect(Collectors.joining(",")));
    }
    
    private boolean versionContainsCollation(final DatabaseMetaData databaseMetaData) throws SQLException {
        return databaseMetaData.getDatabaseMajorVersion() >= COLLATION_START_MAJOR_VERSION && databaseMetaData.getDatabaseMinorVersion() >= COLLATION_START_MINOR_VERSION;
    }
    
    private boolean versionContainsIdentityColumn(final DatabaseMetaData databaseMetaData) throws SQLException {
        return databaseMetaData.getDatabaseMajorVersion() >= COLLATION_START_MAJOR_VERSION && databaseMetaData.getDatabaseMinorVersion() >= IDENTITY_COLUMN_START_MINOR_VERSION;
    }
    
    private Map<String, Collection<IndexMetaData>> loadIndexMetaData(final Connection connection, final Collection<String> tableNames, final String schema) throws SQLException {
        Map<String, Collection<IndexMetaData>> result = new HashMap<>(tableNames.size(), 1F);
        try (PreparedStatement preparedStatement = connection.prepareStatement(getIndexMetaDataSQL(tableNames, schema))) {
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    String indexName = resultSet.getString("INDEX_NAME");
                    String tableName = resultSet.getString("TABLE_NAME");
                    boolean isUnique = "UNIQUE".equals(resultSet.getString("UNIQUENESS"));
                    if (!result.containsKey(tableName)) {
                        result.put(tableName, new LinkedList<>());
                    }
                    IndexMetaData indexMetaData = new IndexMetaData(indexName, loadIndexColumnNames(connection, tableName, indexName, schema));
                    indexMetaData.setUnique(isUnique);
                    result.get(tableName).add(indexMetaData);
                }
            }
        }
        return result;
    }
    
    private List<String> loadIndexColumnNames(final Connection connection, final String tableName, final String indexName, final String schema) throws SQLException {
        try (PreparedStatement preparedStatement = connection.prepareStatement(String.format(INDEX_COLUMN_META_DATA_SQL, schema))) {
            preparedStatement.setString(1, tableName);
            preparedStatement.setString(2, indexName);
            List<String> result = new LinkedList<>();
            ResultSet resultSet = preparedStatement.executeQuery();
            while (resultSet.next()) {
                result.add(resultSet.getString("COLUMN_NAME"));
            }
            return result;
        }
    }
    
    private String getIndexMetaDataSQL(final Collection<String> tableNames, final String schema) {
        // TODO The table name needs to be in uppercase, otherwise the index cannot be found.
        return String.format(INDEX_META_DATA_SQL, schema, tableNames.stream().map(each -> String.format("'%s'", each)).collect(Collectors.joining(",")));
    }
    
    private Map<String, Collection<String>> loadTablePrimaryKeys(final Connection connection, final Collection<String> tableNames, final String schema) throws SQLException {
        Map<String, Collection<String>> result = new HashMap<>();
        try (PreparedStatement preparedStatement = connection.prepareStatement(getPrimaryKeyMetaDataSQL(tableNames, schema))) {
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    String columnName = resultSet.getString("COLUMN_NAME");
                    String tableName = resultSet.getString("TABLE_NAME");
                    result.computeIfAbsent(tableName, key -> new LinkedList<>()).add(columnName);
                }
            }
        }
        return result;
    }
    
    private String getPrimaryKeyMetaDataSQL(final Collection<String> tables, final String schema) {
        return tables.isEmpty() ? String.format(PRIMARY_KEY_META_DATA_SQL, schema)
                : String.format(PRIMARY_KEY_META_DATA_SQL_IN_TABLES, schema, tables.stream().map(each -> String.format("'%s'", each)).collect(Collectors.joining(",")));
    }
    
    @Override
    public String getDatabaseType() {
        return "Oracle";
    }
}
