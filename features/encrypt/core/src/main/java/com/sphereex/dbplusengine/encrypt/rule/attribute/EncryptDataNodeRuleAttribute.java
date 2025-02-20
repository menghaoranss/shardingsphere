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

package com.sphereex.dbplusengine.encrypt.rule.attribute;

import com.cedarsoftware.util.CaseInsensitiveMap;
import com.sphereex.dbplusengine.encrypt.datanode.EncryptTableDataNodeLoader;
import org.apache.shardingsphere.encrypt.rule.table.EncryptTable;
import org.apache.shardingsphere.infra.datanode.DataNode;
import org.apache.shardingsphere.infra.metadata.database.resource.PhysicalDataSourceAggregator;
import org.apache.shardingsphere.infra.rule.ShardingSphereRule;
import org.apache.shardingsphere.infra.rule.attribute.datanode.DataNodeRuleAttribute;

import javax.sql.DataSource;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Encrypt data node rule attribute.
 */
public final class EncryptDataNodeRuleAttribute implements DataNodeRuleAttribute {
    
    private final Map<String, EncryptTable> encryptTables;
    
    private final Map<String, Collection<DataNode>> tableDataNodes;
    
    public EncryptDataNodeRuleAttribute(final String databaseName, final Map<String, DataSource> dataSources,
                                        final Collection<ShardingSphereRule> builtRules, final Map<String, EncryptTable> encryptTables) {
        this.encryptTables = encryptTables;
        tableDataNodes = loadEncryptTableDataNodes(databaseName, dataSources, builtRules, encryptTables);
    }
    
    private Map<String, Collection<DataNode>> loadEncryptTableDataNodes(final String databaseName, final Map<String, DataSource> dataSources,
                                                                        final Collection<ShardingSphereRule> builtRules, final Map<String, EncryptTable> encryptTables) {
        Map<String, Collection<DataNode>> result = new CaseInsensitiveMap<>();
        Map<String, DataSource> aggregateDataSources = PhysicalDataSourceAggregator.getAggregatedDataSources(dataSources, builtRules);
        Collection<String> actualTableNames = encryptTables.values().stream().filter(each -> each.getRenameTable().isPresent()).map(each -> each.getRenameTable().get()).collect(Collectors.toList());
        Map<String, Collection<DataNode>> actualDataNodes = EncryptTableDataNodeLoader.load(databaseName, aggregateDataSources, actualTableNames);
        for (EncryptTable each : encryptTables.values()) {
            if (each.getRenameTable().isPresent() && actualDataNodes.containsKey(each.getRenameTable().get())) {
                result.put(each.getTable(), actualDataNodes.get(each.getRenameTable().get()));
            }
        }
        return result;
    }
    
    @Override
    public Map<String, Collection<DataNode>> getAllDataNodes() {
        return tableDataNodes;
    }
    
    @Override
    public Collection<DataNode> getDataNodesByTableName(final String tableName) {
        return tableDataNodes.getOrDefault(tableName, Collections.emptyList());
    }
    
    @Override
    public Optional<String> findFirstActualTable(final String logicTable) {
        Collection<DataNode> dataNodes = tableDataNodes.get(logicTable);
        return dataNodes.isEmpty() ? Optional.empty() : Optional.of(dataNodes.iterator().next().getTableName());
    }
    
    @Override
    public boolean isNeedAccumulate(final Collection<String> tables) {
        return false;
    }
    
    @Override
    public Optional<String> findLogicTableByActualTable(final String actualTable) {
        return findEncryptTableByActualTable(actualTable).map(EncryptTable::getTable);
    }
    
    private Optional<EncryptTable> findEncryptTableByActualTable(final String actualTableName) {
        for (EncryptTable each : encryptTables.values()) {
            if (each.getRenameTable().orElse("").equalsIgnoreCase(actualTableName)) {
                return Optional.of(each);
            }
        }
        return Optional.empty();
    }
    
    @Override
    public Optional<String> findActualTableByCatalog(final String catalog, final String logicTable) {
        return getDataNodesByTableName(logicTable).stream().filter(each -> each.getDataSourceName().equalsIgnoreCase(catalog)).findFirst().map(DataNode::getTableName);
    }
}
