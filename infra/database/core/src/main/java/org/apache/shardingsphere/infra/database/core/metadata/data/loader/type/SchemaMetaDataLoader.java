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

package org.apache.shardingsphere.infra.database.core.metadata.data.loader.type;

import com.cedarsoftware.util.CaseInsensitiveMap;
import com.cedarsoftware.util.CaseInsensitiveSet;
import com.sphereex.dbplusengine.SphereEx;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.shardingsphere.infra.database.core.metadata.data.loader.MetaDataLoaderConnection;
import org.apache.shardingsphere.infra.database.core.metadata.database.DialectDatabaseMetaData;
import org.apache.shardingsphere.infra.database.core.metadata.database.system.SystemDatabase;
import org.apache.shardingsphere.infra.database.core.type.DatabaseType;
import org.apache.shardingsphere.infra.database.core.type.DatabaseTypeRegistry;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;

/**
 * Schema meta data loader.
 */
@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class SchemaMetaDataLoader {
    
    private static final String TABLE_TYPE = "TABLE";
    
    private static final String PARTITIONED_TABLE_TYPE = "PARTITIONED TABLE";
    
    private static final String VIEW_TYPE = "VIEW";
    
    private static final String SYSTEM_TABLE_TYPE = "SYSTEM TABLE";
    
    private static final String SYSTEM_VIEW_TYPE = "SYSTEM VIEW";
    
    private static final String TABLE_NAME = "TABLE_NAME";
    
    private static final String TABLE_SCHEME = "TABLE_SCHEM";
    
    @SphereEx
    private static final String SYNONYM_TYPE = "SYNONYM";
    
    @SphereEx
    private static final String QUERY_INVALID_VIEW_SQL = "SELECT OBJECT_NAME as VIEW_NAME FROM ALL_OBJECTS where OWNER='%s' and STATUS='INVALID' and OBJECT_TYPE='VIEW'";
    
    @SphereEx
    private static final String PUBLIC_SYNONYMS_TABLE_SQL = "SELECT SYNONYM_NAME FROM ALL_SYNONYMS WHERE OWNER = 'PUBLIC' AND TABLE_OWNER NOT IN "
            + "('MDSYS','OWBSYS','OLAPSYS','CTXSYS','FLOWS_FILES','APEX_030200','EXFSYS','SYSTEM','DBSNMP','ORDSYS','SYSMAN','XDB','ORDDATA','APPQOSSYS','SYS','WMSYS')";
    
    /**
     * Load schema table names.
     *
     * @param databaseName database name
     * @param databaseType database type
     * @param dataSource data source
     * @param includedTables included tables
     * @param loadMetaDataSchemas load metadata schemas
     * @param excludedTables excluded tables
     * @return loaded schema table names
     * @throws SQLException SQL exception
     */
    public static Map<String, Collection<String>> loadSchemaTableNames(final String databaseName, final DatabaseType databaseType, final DataSource dataSource,
                                                                       @SphereEx final Collection<String> includedTables, @SphereEx final Collection<String> loadMetaDataSchemas,
                                                                       final Collection<String> excludedTables) throws SQLException {
        try (MetaDataLoaderConnection connection = new MetaDataLoaderConnection(databaseType, dataSource.getConnection())) {
            Collection<String> schemaNames = loadSchemaNames(connection, databaseType);
            if (!loadMetaDataSchemas.isEmpty()) {
                schemaNames.addAll(loadMetaDataSchemas);
            }
            schemaNames = replaceSchemaName(schemaNames, databaseType);
            DialectDatabaseMetaData dialectDatabaseMetaData = new DatabaseTypeRegistry(databaseType).getDialectDatabaseMetaData();
            Map<String, Collection<String>> result = new CaseInsensitiveMap<>(schemaNames.size(), 1F);
            for (String each : schemaNames) {
                String schemaName = dialectDatabaseMetaData.getDefaultSchema().isPresent() ? each : databaseName;
                // SPEX CHANGED: BEGIN
                result.put(schemaName, loadValidTableNames(connection, each, includedTables, excludedTables, databaseType));
                // SPEX CHANGED: END
            }
            if (!containsTableMetaData(result)) {
                log.warn(String.format("Not found table metadata in schema (%s), please check your schema or config load-metadata-schema", schemaNames));
            }
            return result;
        }
    }
    
    private static Collection<String> replaceSchemaName(final Collection<String> schemaNames, final DatabaseType databaseType) {
        Collection<String> result = new LinkedList<>();
        for (String each : schemaNames) {
            result.add(each);
            String replaceSchemaName = replaceSchemaName(each, databaseType);
            if (!result.contains(replaceSchemaName)) {
                result.add(replaceSchemaName);
            }
        }
        return result;
    }
    
    private static String replaceSchemaName(final String schemaName, final DatabaseType databaseType) {
        if (!"Oracle".equalsIgnoreCase(databaseType.getType()) && !"Oceanbase_Oracle".equalsIgnoreCase(databaseType.getType())) {
            return schemaName;
        }
        if (schemaName.endsWith("OPR")) {
            return schemaName.substring(0, schemaName.length() - 3) + "DATA";
        }
        return schemaName;
    }
    
    private static boolean containsTableMetaData(final Map<String, Collection<String>> schemaTableNames) {
        boolean result = false;
        for (Entry<String, Collection<String>> entry : schemaTableNames.entrySet()) {
            if (!entry.getValue().isEmpty()) {
                result = true;
            }
        }
        return result;
    }
    
    /**
     * Load schema names.
     *
     * @param connection connection
     * @param databaseType database type
     * @return schema names collection
     * @throws SQLException SQL exception
     */
    public static Collection<String> loadSchemaNames(final Connection connection, final DatabaseType databaseType) throws SQLException {
        DialectDatabaseMetaData dialectDatabaseMetaData = new DatabaseTypeRegistry(databaseType).getDialectDatabaseMetaData();
        // SPEX CHANGED: BEGIN
        if (!dialectDatabaseMetaData.getDefaultSchema().isPresent() || isHiveOrPrestoDatabase(databaseType)) {
            // SPEX CHANGED: END
            Collection<String> result = new LinkedList<>();
            result.add(connection.getSchema());
            return result;
        }
        Collection<String> result = new LinkedList<>();
        SystemDatabase systemDatabase = new SystemDatabase(databaseType);
        try (ResultSet resultSet = connection.getMetaData().getSchemas()) {
            while (resultSet.next()) {
                String schema = resultSet.getString(TABLE_SCHEME);
                if (!systemDatabase.getSystemSchemas().contains(schema)) {
                    result.add(schema);
                }
            }
        }
        return result.isEmpty() ? Collections.singletonList(connection.getSchema()) : result;
    }
    
    @SphereEx
    private static boolean isHiveOrPrestoDatabase(final DatabaseType databaseType) {
        return "Hive".equals(databaseType.getType()) || "Presto".equals(databaseType.getType());
    }
    
    @SphereEx
    private static Collection<String> loadValidTableNames(final Connection connection, final String schemaName, final Collection<String> includedTables,
                                                          final Collection<String> excludedTables, final DatabaseType databaseType) throws SQLException {
        Collection<String> result = loadTableNames(connection, schemaName, includedTables, excludedTables, databaseType);
        if ("Oracle".equals(databaseType.getType()) || "OceanBase_Oracle".equals(databaseType.getType())) {
            result.addAll(loadPublicSynonyms(connection, includedTables, excludedTables));
        }
        if (isOracleDatabase(databaseType)) {
            result = filterOracleInvalidView(connection, result);
        }
        return result;
    }
    
    @SphereEx
    private static Collection<String> loadPublicSynonyms(final Connection connection, final Collection<String> includedTables, final Collection<String> excludedTables) throws SQLException {
        Collection<String> result = new CaseInsensitiveSet<>();
        try (Statement statement = connection.createStatement()) {
            try (ResultSet resultSet = statement.executeQuery(PUBLIC_SYNONYMS_TABLE_SQL)) {
                while (resultSet.next()) {
                    String table = resultSet.getString(1);
                    if (!includedTables.isEmpty() && !includedTables.contains(table)) {
                        continue;
                    }
                    if (!isSystemTable(table) && !excludedTables.contains(table)) {
                        result.add(table);
                    }
                }
            }
        }
        return result;
    }
    
    @SphereEx
    private static boolean isOracleDatabase(final DatabaseType databaseType) {
        return "Oracle".equals(databaseType.getType()) || "Oceanbase_Oracle".equals(databaseType.getType());
    }
    
    @SphereEx
    private static Collection<String> filterOracleInvalidView(final Connection connection, final Collection<String> tableNames) throws SQLException {
        Collection<String> invalidViews = new HashSet<>();
        try (
                Statement statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery(String.format(QUERY_INVALID_VIEW_SQL, connection.getSchema()))) {
            while (resultSet.next()) {
                String viewName = resultSet.getString("VIEW_NAME");
                invalidViews.add(viewName);
            }
        }
        return tableNames.stream().filter(tableName -> !invalidViews.contains(tableName)).collect(Collectors.toList());
    }
    
    private static Collection<String> loadTableNames(final Connection connection, final String schemaName, @SphereEx final Collection<String> includedTables,
                                                     final Collection<String> excludedTables, @SphereEx final DatabaseType databaseType) throws SQLException {
        Collection<String> result = new LinkedList<>();
        // SPEX CHANGED: BEGIN
        String[] tableTypes = isOracleDatabase(databaseType)
                ? new String[]{TABLE_TYPE, PARTITIONED_TABLE_TYPE, VIEW_TYPE, SYSTEM_TABLE_TYPE, SYSTEM_VIEW_TYPE, SYNONYM_TYPE}
                : new String[]{TABLE_TYPE, PARTITIONED_TABLE_TYPE, VIEW_TYPE, SYSTEM_TABLE_TYPE, SYSTEM_VIEW_TYPE};
        try (
                ResultSet resultSet = connection.getMetaData().getTables(connection.getCatalog(), schemaName, null, tableTypes)) {
            // SPEX CHANGED: END
            while (resultSet.next()) {
                String table = resultSet.getString(TABLE_NAME);
                // SPEX ADDED: BEGIN
                if (!includedTables.isEmpty() && !includedTables.contains(table)) {
                    continue;
                }
                // SPEX ADDED: END
                if (!isSystemTable(table) && !excludedTables.contains(table)) {
                    result.add(table);
                }
            }
        }
        return result;
    }
    
    private static boolean isSystemTable(final String table) {
        return table.contains("$") || table.contains("/") || table.contains("##");
    }
}
