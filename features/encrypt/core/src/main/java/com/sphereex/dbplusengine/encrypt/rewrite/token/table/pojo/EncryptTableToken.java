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

package com.sphereex.dbplusengine.encrypt.rewrite.token.table.pojo;

import com.cedarsoftware.util.CaseInsensitiveMap;
import com.cedarsoftware.util.CaseInsensitiveSet;
import lombok.Getter;
import org.apache.shardingsphere.infra.database.core.metadata.database.DialectDatabaseMetaData;
import org.apache.shardingsphere.infra.database.core.type.DatabaseTypeRegistry;
import org.apache.shardingsphere.infra.database.oracle.type.OracleDatabaseType;
import org.apache.shardingsphere.infra.datanode.DataNode;
import org.apache.shardingsphere.infra.datanode.DataNodes;
import org.apache.shardingsphere.infra.exception.generic.UnsupportedSQLOperationException;
import org.apache.shardingsphere.infra.metadata.database.ShardingSphereDatabase;
import org.apache.shardingsphere.infra.metadata.database.resource.unit.StorageUnit;
import org.apache.shardingsphere.infra.rewrite.sql.token.common.pojo.RouteUnitAware;
import org.apache.shardingsphere.infra.rewrite.sql.token.common.pojo.SQLToken;
import org.apache.shardingsphere.infra.rewrite.sql.token.common.pojo.Substitutable;
import org.apache.shardingsphere.infra.route.context.RouteMapper;
import org.apache.shardingsphere.infra.route.context.RouteUnit;
import org.apache.shardingsphere.sql.parser.statement.core.value.identifier.IdentifierValue;

import java.util.Collection;
import java.util.Map;
import java.util.Optional;

/**
 * Encrypt table token.
 */
public final class EncryptTableToken extends SQLToken implements Substitutable, RouteUnitAware {
    
    private final Collection<DataNode> dataNodes;
    
    private final Collection<String> dataSourceNames;
    
    @Getter
    private final int stopIndex;
    
    @Getter
    private final IdentifierValue tableName;
    
    private final Map<String, StorageUnit> storageUnits;
    
    public EncryptTableToken(final int startIndex, final int stopIndex, final IdentifierValue tableName, final ShardingSphereDatabase database) {
        super(startIndex);
        this.stopIndex = stopIndex;
        this.tableName = tableName;
        storageUnits = database.getResourceMetaData().getStorageUnits();
        dataNodes = new DataNodes(database.getRuleMetaData().getRules()).getDataNodes(tableName.getValue());
        dataSourceNames = new CaseInsensitiveSet<>(dataNodes.size(), 1F);
        dataNodes.forEach(each -> dataSourceNames.add(each.getDataSourceName()));
    }
    
    @Override
    public String toString(final RouteUnit routeUnit) {
        String actualDataSourceName = routeUnit.getDataSourceMapper().getActualName();
        StorageUnit routeStorageUnit = storageUnits.get(actualDataSourceName);
        DialectDatabaseMetaData dialectDatabaseMetaData = new DatabaseTypeRegistry(routeStorageUnit.getStorageType()).getDialectDatabaseMetaData();
        Map<String, String> logicActualTableMap = getLogicActualTableMap(routeUnit);
        if (!dialectDatabaseMetaData.isInstanceConnectionAvailable() || dataNodes.isEmpty()) {
            return tableName.getQuoteCharacter().wrap(logicActualTableMap.getOrDefault(tableName.getValue(), tableName.getValue()));
        }
        if (dataSourceNames.contains(actualDataSourceName)) {
            return getValueWithQuoteCharacters(routeStorageUnit, dialectDatabaseMetaData, logicActualTableMap);
        }
        for (DataNode each : dataNodes) {
            StorageUnit targetStorageUnit = storageUnits.get(each.getDataSourceName());
            if (routeStorageUnit.getConnectionProperties().isInSameDatabaseInstance(targetStorageUnit.getConnectionProperties())) {
                return getValueWithQuoteCharacters(targetStorageUnit, dialectDatabaseMetaData, logicActualTableMap);
            }
        }
        throw new UnsupportedSQLOperationException("all encrypt tables must be in same database or instance");
    }
    
    private Map<String, String> getLogicActualTableMap(final RouteUnit routeUnit) {
        Map<String, String> result = new CaseInsensitiveMap<>();
        for (RouteMapper each : routeUnit.getTableMappers()) {
            result.put(each.getLogicName(), each.getActualName());
        }
        return result;
    }
    
    private String getValueWithQuoteCharacters(final StorageUnit storageUnit, final DialectDatabaseMetaData dialectDatabaseMetaData, final Map<String, String> logicActualTableMap) {
        String schema = Optional.ofNullable(storageUnit.getConnectionProperties().getSchema()).orElseGet(() -> storageUnit.getConnectionProperties().getCatalog());
        String caseConvertedSchema = storageUnit.getStorageType() instanceof OracleDatabaseType ? schema.toUpperCase() : schema;
        String warpedSchema = dialectDatabaseMetaData.getQuoteCharacter().wrap(caseConvertedSchema);
        return warpedSchema + "." + tableName.getQuoteCharacter().wrap(logicActualTableMap.getOrDefault(tableName.getValue(), tableName.getValue()));
    }
}
