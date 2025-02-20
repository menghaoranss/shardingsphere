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

package org.apache.shardingsphere.infra.rewrite.sql.token.common.pojo.generic;

import com.cedarsoftware.util.CaseInsensitiveMap;
import com.cedarsoftware.util.CaseInsensitiveSet;
import com.google.common.base.Joiner;
import com.sphereex.dbplusengine.SphereEx;
import lombok.Getter;
import org.apache.shardingsphere.infra.binder.context.segment.select.projection.Projection;
import org.apache.shardingsphere.infra.binder.context.segment.select.projection.impl.ColumnProjection;
import org.apache.shardingsphere.infra.database.core.metadata.database.DialectDatabaseMetaData;
import org.apache.shardingsphere.infra.database.core.metadata.database.enums.QuoteCharacter;
import org.apache.shardingsphere.infra.database.core.type.DatabaseType;
import org.apache.shardingsphere.infra.database.core.type.DatabaseTypeRegistry;
import org.apache.shardingsphere.infra.datanode.DataNode;
import org.apache.shardingsphere.infra.datanode.DataNodes;
import org.apache.shardingsphere.infra.exception.kernel.metadata.resource.storageunit.MissingRequiredStorageUnitsException;
import org.apache.shardingsphere.infra.metadata.ShardingSphereMetaData;
import org.apache.shardingsphere.infra.metadata.database.ShardingSphereDatabase;
import org.apache.shardingsphere.infra.metadata.database.resource.unit.StorageUnit;
import org.apache.shardingsphere.infra.rewrite.sql.token.common.pojo.RouteUnitAware;
import org.apache.shardingsphere.infra.rewrite.sql.token.common.pojo.SQLToken;
import org.apache.shardingsphere.infra.rewrite.sql.token.common.pojo.Substitutable;
import org.apache.shardingsphere.infra.route.context.RouteMapper;
import org.apache.shardingsphere.infra.route.context.RouteUnit;
import org.apache.shardingsphere.infra.rule.ShardingSphereRule;
import org.apache.shardingsphere.sql.parser.statement.core.segment.generic.bound.ColumnSegmentBoundInfo;
import org.apache.shardingsphere.sql.parser.statement.core.value.identifier.IdentifierValue;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Substitutable column name token.
 */
public final class SubstitutableColumnNameToken extends SQLToken implements Substitutable, RouteUnitAware {
    
    private static final String COLUMN_NAME_SPLITTER = ", ";
    
    @Getter
    private final int stopIndex;
    
    @Getter
    private final Collection<Projection> projections;
    
    private final QuoteCharacter quoteCharacter;
    
    @Getter
    private final DatabaseType databaseType;
    
    @SphereEx
    private final ShardingSphereMetaData metaData;
    
    @SphereEx
    private final ShardingSphereDatabase database;
    
    @SphereEx
    private final boolean isShorthand;
    
    public SubstitutableColumnNameToken(final int startIndex, final int stopIndex, final Collection<Projection> projections, final DatabaseType databaseType,
                                        final ShardingSphereDatabase database, final ShardingSphereMetaData metaData) {
        super(startIndex);
        this.stopIndex = stopIndex;
        quoteCharacter = new DatabaseTypeRegistry(databaseType).getDialectDatabaseMetaData().getQuoteCharacter();
        this.projections = projections;
        this.databaseType = databaseType;
        // SPEX ADDED: BEGIN
        isShorthand = false;
        this.metaData = metaData;
        this.database = database;
        // SPEX ADDED: END
    }
    
    @SphereEx
    public SubstitutableColumnNameToken(final int startIndex, final int stopIndex, final Collection<Projection> projections, final DatabaseType databaseType,
                                        final ShardingSphereDatabase database, final ShardingSphereMetaData metaData, final boolean isShorthand) {
        super(startIndex);
        this.stopIndex = stopIndex;
        quoteCharacter = new DatabaseTypeRegistry(databaseType).getDialectDatabaseMetaData().getQuoteCharacter();
        this.projections = projections;
        this.databaseType = databaseType;
        this.metaData = metaData;
        this.database = database;
        this.isShorthand = isShorthand;
    }
    
    @Override
    public String toString(final RouteUnit routeUnit) {
        Map<String, String> logicAndActualTables = getLogicAndActualTables(routeUnit);
        StringBuilder result = new StringBuilder();
        int index = 0;
        for (Projection each : projections) {
            if (index > 0) {
                result.append(COLUMN_NAME_SPLITTER);
            }
            // SPEX CHANGED: BEGIN
            result.append(getColumnExpression(each, logicAndActualTables, routeUnit));
            // SPEX CHANGED: END
            index++;
        }
        // SPEX ADDED: BEGIN
        if (isShorthand) {
            result.append(" ");
        }
        // SPEX ADDED: END
        return result.toString();
    }
    
    private Map<String, String> getLogicAndActualTables(final RouteUnit routeUnit) {
        if (null == routeUnit) {
            return Collections.emptyMap();
        }
        Map<String, String> result = new CaseInsensitiveMap<>();
        for (RouteMapper each : routeUnit.getTableMappers()) {
            result.put(each.getLogicName(), each.getActualName());
        }
        return result;
    }
    
    private String getColumnExpression(final Projection projection, final Map<String, String> logicActualTableNames, @SphereEx final RouteUnit routeUnit) {
        StringBuilder builder = new StringBuilder();
        if (projection instanceof ColumnProjection) {
            // SPEX CHANGED: BEGIN
            appendColumnProjection((ColumnProjection) projection, logicActualTableNames, builder, routeUnit);
            // SPEX CHANGED: END
        } else {
            builder.append(quoteCharacter.wrap(projection.getColumnLabel()));
        }
        return builder.toString();
    }
    
    private void appendColumnProjection(final ColumnProjection columnProjection, final Map<String, String> logicActualTableNames, final StringBuilder builder, @SphereEx final RouteUnit routeUnit) {
        columnProjection.getLeftParentheses().ifPresent(optional -> builder.append("("));
        // SPEX ADDED: BEGIN
        columnProjection.getWrappedUDFNames().descendingIterator().forEachRemaining(each -> builder.append(each).append("("));
        // SPEX ADDED: END
        if (columnProjection.getOwner().isPresent()) {
            IdentifierValue owner = columnProjection.getOwner().get();
            // SPEX ADDED: BEGIN
            appendColumnTableOwnerSchema(logicActualTableNames, owner.getValue(), builder, routeUnit, columnProjection.getColumnBoundInfo());
            // SPEX ADDED: END
            String actualTableOwner = logicActualTableNames.getOrDefault(owner.getValue(), owner.getValue());
            builder.append(getValueWithQuoteCharacters(new IdentifierValue(actualTableOwner, owner.getQuoteCharacter()))).append('.');
        }
        builder.append(getValueWithQuoteCharacters(columnProjection.getName()));
        // SPEX ADDED: BEGIN
        columnProjection.getWrappedUDFNames().forEach(each -> builder.append(each).append(")"));
        // SPEX ADDED: END
        columnProjection.getRightParentheses().ifPresent(optional -> builder.append(")"));
        if (columnProjection.getAlias().isPresent()) {
            builder.append(" AS ").append(getValueWithQuoteCharacters(columnProjection.getAlias().get()));
        }
        // SPEX ADDED: BEGIN
        if (columnProjection.isEncryptColumnContainsInGroupByItem()) {
            builder.insert(0, "MIN(").append(")");
        }
        // SPEX ADDED: END
    }
    
    @SphereEx
    private void appendColumnTableOwnerSchema(final Map<String, String> logicActualTableNames, final String owner,
                                              final StringBuilder builder, final RouteUnit routeUnit, final ColumnSegmentBoundInfo columnBoundInfo) {
        String databaseName = Optional.ofNullable(columnBoundInfo).map(optional -> optional.getOriginalDatabase().getValue()).orElse(database.getName());
        if (!metaData.containsDatabase(databaseName) || !logicActualTableNames.containsKey(owner)) {
            return;
        }
        ShardingSphereDatabase database = metaData.getDatabase(databaseName);
        String actualDataSourceName = routeUnit.getDataSourceMapper().getActualName();
        Map<String, StorageUnit> storageUnits = database.getResourceMetaData().getStorageUnits();
        StorageUnit routeStorageUnit = getStorageUnit(storageUnits, actualDataSourceName);
        DialectDatabaseMetaData dialectDatabaseMetaData = new DatabaseTypeRegistry(routeStorageUnit.getStorageType()).getDialectDatabaseMetaData();
        if (!dialectDatabaseMetaData.isInstanceConnectionAvailable()) {
            return;
        }
        Collection<ShardingSphereRule> rules = database.getRuleMetaData().getRules();
        Collection<DataNode> dataNodes = new DataNodes(rules).getDataNodes(owner);
        Collection<String> dataSourceNames = getTableDataSourceNames(dataNodes);
        if (dataSourceNames.contains(actualDataSourceName)) {
            builder.append(getWarpedSchema(routeStorageUnit, dialectDatabaseMetaData)).append(".");
            return;
        }
        for (DataNode each : dataNodes) {
            StorageUnit targetStorageUnit = getStorageUnit(storageUnits, each.getDataSourceName());
            if (routeStorageUnit.getConnectionProperties().isInSameDatabaseInstance(targetStorageUnit.getConnectionProperties())) {
                builder.append(getWarpedSchema(targetStorageUnit, dialectDatabaseMetaData)).append(".");
                return;
            }
        }
    }
    
    private StorageUnit getStorageUnit(final Map<String, StorageUnit> storageUnits, final String actualDataSourceName) {
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
    
    @SphereEx
    private Collection<String> getTableDataSourceNames(final Collection<DataNode> dataNodes) {
        Collection<String> result = new CaseInsensitiveSet<>(dataNodes.size(), 1F);
        dataNodes.forEach(each -> result.add(each.getDataSourceName()));
        return result;
    }
    
    @SphereEx
    private String getWarpedSchema(final StorageUnit storageUnit, final DialectDatabaseMetaData dialectDatabaseMetaData) {
        String schema = Optional.ofNullable(storageUnit.getConnectionProperties().getSchema()).orElseGet(() -> storageUnit.getConnectionProperties().getCatalog());
        String formattedSchema = dialectDatabaseMetaData.formatTableNamePattern(schema);
        return dialectDatabaseMetaData.getQuoteCharacter().wrap(formattedSchema);
    }
    
    private String getValueWithQuoteCharacters(final IdentifierValue identifierValue) {
        return QuoteCharacter.NONE == identifierValue.getQuoteCharacter() ? identifierValue.getValue() : quoteCharacter.wrap(identifierValue.getValue());
    }
    
    @SphereEx
    public Collection<Projection> getUnmodifiableProjections() {
        return Collections.unmodifiableCollection(projections);
    }
}
