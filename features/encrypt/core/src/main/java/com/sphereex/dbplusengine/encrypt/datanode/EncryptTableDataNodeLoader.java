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

import com.cedarsoftware.util.CaseInsensitiveMap;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.SneakyThrows;
import org.apache.shardingsphere.infra.database.DatabaseTypeEngine;
import org.apache.shardingsphere.infra.database.core.metadata.data.loader.type.SchemaMetaDataLoader;
import org.apache.shardingsphere.infra.database.core.type.DatabaseType;
import org.apache.shardingsphere.infra.datanode.DataNode;

import javax.sql.DataSource;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Encrypt table data node loader.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class EncryptTableDataNodeLoader {
    
    /**
     * Load encrypt table data nodes.
     *
     * @param databaseName database name
     * @param dataSourceMap data source map
     * @param includedTables included tables
     * @return encrypt table data node map
     */
    public static Map<String, Collection<DataNode>> load(final String databaseName, final Map<String, DataSource> dataSourceMap, final Collection<String> includedTables) {
        Map<String, Collection<DataNode>> result = new CaseInsensitiveMap<>();
        for (Entry<String, DataSource> entry : dataSourceMap.entrySet()) {
            Map<String, Collection<DataNode>> dataNodeMap = load(databaseName, DatabaseTypeEngine.getStorageType(entry.getValue()), entry.getKey(), entry.getValue(), includedTables);
            for (Entry<String, Collection<DataNode>> each : dataNodeMap.entrySet()) {
                Collection<DataNode> addedDataNodes = each.getValue();
                Collection<DataNode> existDataNodes = result.computeIfAbsent(each.getKey().toLowerCase(), unused -> new LinkedHashSet<>(addedDataNodes.size(), 1F));
                existDataNodes.addAll(addedDataNodes);
            }
        }
        return result;
    }
    
    @SneakyThrows
    private static Map<String, Collection<DataNode>> load(final String databaseName, final DatabaseType storageType, final String dataSourceName,
                                                          final DataSource dataSource, final Collection<String> includedTables) {
        Map<String, Collection<String>> schemaTableNames = SchemaMetaDataLoader.loadSchemaTableNames(databaseName, storageType, dataSource, includedTables, Collections.emptyList());
        Map<String, Collection<DataNode>> result = new CaseInsensitiveMap<>();
        for (Entry<String, Collection<String>> entry : schemaTableNames.entrySet()) {
            for (String each : entry.getValue()) {
                Collection<DataNode> dataNodes = result.computeIfAbsent(each, unused -> new LinkedList<>());
                DataNode dataNode = new DataNode(dataSourceName, each);
                dataNode.setSchemaName(entry.getKey());
                dataNodes.add(dataNode);
            }
        }
        return result;
    }
}
