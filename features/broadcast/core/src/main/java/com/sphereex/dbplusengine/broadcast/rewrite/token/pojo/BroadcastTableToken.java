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

package com.sphereex.dbplusengine.broadcast.rewrite.token.pojo;

import com.cedarsoftware.util.CaseInsensitiveSet;
import lombok.Getter;
import org.apache.shardingsphere.infra.database.core.type.DatabaseType;
import org.apache.shardingsphere.infra.database.core.type.DatabaseTypeRegistry;
import org.apache.shardingsphere.infra.database.oracle.type.OracleDatabaseType;
import org.apache.shardingsphere.infra.datanode.DataNode;
import org.apache.shardingsphere.infra.datanode.DataNodes;
import org.apache.shardingsphere.infra.metadata.database.ShardingSphereDatabase;
import org.apache.shardingsphere.infra.metadata.database.resource.unit.StorageUnit;
import org.apache.shardingsphere.infra.rewrite.sql.token.common.pojo.RouteUnitAware;
import org.apache.shardingsphere.infra.rewrite.sql.token.common.pojo.SQLToken;
import org.apache.shardingsphere.infra.rewrite.sql.token.common.pojo.Substitutable;
import org.apache.shardingsphere.infra.route.context.RouteUnit;
import org.apache.shardingsphere.sql.parser.statement.core.value.identifier.IdentifierValue;

import java.util.Collection;
import java.util.Map;
import java.util.Optional;

/**
 * Broadcast table token.
 */
public final class BroadcastTableToken extends SQLToken implements Substitutable, RouteUnitAware {
    
    private final Collection<String> dataSourceNames;
    
    @Getter
    private final int stopIndex;
    
    private final IdentifierValue tableName;
    
    private final Map<String, StorageUnit> storageUnits;
    
    private final DatabaseType databaseType;
    
    public BroadcastTableToken(final int startIndex, final int stopIndex, final IdentifierValue tableName, final ShardingSphereDatabase database, final DatabaseType databaseType) {
        super(startIndex);
        this.stopIndex = stopIndex;
        this.tableName = tableName;
        this.storageUnits = database.getResourceMetaData().getStorageUnits();
        this.databaseType = databaseType;
        Collection<DataNode> dataNodes = new DataNodes(database.getRuleMetaData().getRules()).getDataNodes(tableName.getValue());
        dataSourceNames = new CaseInsensitiveSet<>(dataNodes.size(), 1F);
        dataNodes.forEach(each -> dataSourceNames.add(each.getDataSourceName()));
    }
    
    @Override
    public String toString(final RouteUnit routeUnit) {
        String actualDataSourceName = routeUnit.getDataSourceMapper().getActualName();
        StorageUnit routeStorageUnit = storageUnits.get(actualDataSourceName);
        if (new DatabaseTypeRegistry(routeStorageUnit.getStorageType()).getDialectDatabaseMetaData().isInstanceConnectionAvailable() && dataSourceNames.contains(actualDataSourceName)) {
            return getSchemaValue(routeStorageUnit) + "." + getActualTableName();
        }
        return getActualTableName();
    }
    
    private String getSchemaValue(final StorageUnit storageUnit) {
        String schema = Optional.ofNullable(storageUnit.getConnectionProperties().getSchema()).orElseGet(() -> storageUnit.getConnectionProperties().getCatalog());
        String caseConvertedSchema = storageUnit.getStorageType() instanceof OracleDatabaseType ? schema.toUpperCase() : schema;
        return new DatabaseTypeRegistry(databaseType).getDialectDatabaseMetaData().getQuoteCharacter().wrap(caseConvertedSchema);
    }
    
    private String getActualTableName() {
        return tableName.getQuoteCharacter().wrap(tableName.getValue());
    }
}
