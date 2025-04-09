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

package com.sphereex.dbplusengine.single.rewrite.token.pojo;

import com.cedarsoftware.util.CaseInsensitiveSet;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import lombok.Getter;
import org.apache.shardingsphere.infra.config.props.ConfigurationPropertyKey;
import org.apache.shardingsphere.infra.database.core.metadata.database.DialectDatabaseMetaData;
import org.apache.shardingsphere.infra.database.core.type.DatabaseTypeRegistry;
import org.apache.shardingsphere.infra.database.oracle.type.OracleDatabaseType;
import org.apache.shardingsphere.infra.datanode.DataNode;
import org.apache.shardingsphere.infra.datanode.DataNodes;
import org.apache.shardingsphere.infra.exception.generic.UnsupportedSQLOperationException;
import org.apache.shardingsphere.infra.exception.kernel.metadata.resource.storageunit.MissingRequiredStorageUnitsException;
import org.apache.shardingsphere.infra.metadata.ShardingSphereMetaData;
import org.apache.shardingsphere.infra.metadata.database.ShardingSphereDatabase;
import org.apache.shardingsphere.infra.metadata.database.resource.unit.StorageUnit;
import org.apache.shardingsphere.infra.rewrite.sql.token.common.pojo.RouteUnitAware;
import org.apache.shardingsphere.infra.rewrite.sql.token.common.pojo.SQLToken;
import org.apache.shardingsphere.infra.rewrite.sql.token.common.pojo.Substitutable;
import org.apache.shardingsphere.infra.route.context.RouteUnit;
import org.apache.shardingsphere.sql.parser.statement.core.value.identifier.IdentifierValue;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.LinkedList;
import java.util.stream.Collectors;

/**
 * Single table token.
 */
public final class SingleTableToken extends SQLToken implements Substitutable, RouteUnitAware {
    
    private final Collection<DataNode> dataNodes;
    
    private final Collection<String> dataSourceNames;
    
    @Getter
    private final int stopIndex;
    
    @Getter
    private final IdentifierValue tableName;
    
    private final Map<String, StorageUnit> storageUnits;
    
    private final ShardingSphereMetaData metaData;
    
    private final Collection<String> loadMetaDataSchema;
    
    public SingleTableToken(final int startIndex, final int stopIndex, final IdentifierValue tableName, final ShardingSphereDatabase database, final ShardingSphereMetaData metaData) {
        super(startIndex);
        this.stopIndex = stopIndex;
        this.tableName = tableName;
        this.metaData = metaData;
        storageUnits = database.getResourceMetaData().getStorageUnits();
        dataNodes = new DataNodes(database.getRuleMetaData().getRules()).getDataNodes(tableName.getValue());
        dataSourceNames = new CaseInsensitiveSet<>(dataNodes.size(), 1F);
        dataNodes.forEach(each -> dataSourceNames.add(each.getDataSourceName()));
        loadMetaDataSchema = getSchemas(database);
    }
    
    private Collection<String> getSchemas(final ShardingSphereDatabase database) {
        Collection<String> result = new LinkedList<>();
        Collection<String> schemas = Splitter.on(",").omitEmptyStrings().trimResults().splitToList(metaData.getProps().getValue(ConfigurationPropertyKey.LOAD_METADATA_SCHEMA));
        if (!schemas.contains(database.getName())) {
            result.add(database.getName());
        }
        return result;
    }
    
    @Override
    public String toString(final RouteUnit routeUnit) {
        String actualDataSourceName = routeUnit.getDataSourceMapper().getActualName();
        StorageUnit routeStorageUnit = getStorageUnit(actualDataSourceName);
        DialectDatabaseMetaData dialectDatabaseMetaData = new DatabaseTypeRegistry(routeStorageUnit.getStorageType()).getDialectDatabaseMetaData();
        if (!dialectDatabaseMetaData.isInstanceConnectionAvailable()) {
            return tableName.getValueWithQuoteCharacters();
        }
        if (dataSourceNames.contains(actualDataSourceName)) {
            return getValueWithQuoteCharacters(routeStorageUnit, dialectDatabaseMetaData);
        }
        for (DataNode each : dataNodes) {
            StorageUnit targetStorageUnit = getStorageUnit(each.getDataSourceName());
            if (routeStorageUnit.getConnectionProperties().isInSameDatabaseInstance(targetStorageUnit.getConnectionProperties())) {
                return getValueWithQuoteCharacters(targetStorageUnit, dialectDatabaseMetaData);
            }
        }
        throw new UnsupportedSQLOperationException("all single tables must be in the same compute node");
    }
    
    private StorageUnit getStorageUnit(final String actualDataSourceName) {
        if (storageUnits.containsKey(actualDataSourceName)) {
            return storageUnits.get(actualDataSourceName);
        }
        for (ShardingSphereDatabase each : metaData.getAllDatabases()) {
            if (each.getResourceMetaData().getStorageUnits().containsKey(actualDataSourceName)) {
                return each.getResourceMetaData().getStorageUnits().get(actualDataSourceName);
            }
        }
        throw new MissingRequiredStorageUnitsException(Joiner.on(", ").join(metaData.getAllDatabases().stream()
                .map(ShardingSphereDatabase::getName).collect(Collectors.toList())), Collections.singleton(actualDataSourceName));
    }
    
    private String getValueWithQuoteCharacters(final StorageUnit storageUnit, final DialectDatabaseMetaData dialectDatabaseMetaData) {
        String schema = Optional.ofNullable(storageUnit.getConnectionProperties().getSchema()).orElseGet(() -> storageUnit.getConnectionProperties().getCatalog());
        String caseConvertedSchema = storageUnit.getStorageType() instanceof OracleDatabaseType ? schema.toUpperCase() : schema;
        String warpedSchema = dialectDatabaseMetaData.getQuoteCharacter().wrap(caseConvertedSchema);
        if (loadMetaDataSchema.isEmpty()) {
            return warpedSchema + "." + tableName.getValueWithQuoteCharacters();
        }
        String dataNodeSchema = dataNodes.iterator().next().getSchemaName();
        String result = loadMetaDataSchema.contains(dataNodeSchema) ? dataNodeSchema : warpedSchema;
        return result + "." + tableName.getValueWithQuoteCharacters();
    }
}
