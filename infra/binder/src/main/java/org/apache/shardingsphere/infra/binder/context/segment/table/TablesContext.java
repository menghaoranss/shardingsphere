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

package org.apache.shardingsphere.infra.binder.context.segment.table;

import com.cedarsoftware.util.CaseInsensitiveSet;
import com.google.common.base.Strings;
import com.sphereex.dbplusengine.SphereEx;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.ToString;
import org.apache.shardingsphere.infra.binder.context.segment.select.subquery.SubqueryTableContext;
import org.apache.shardingsphere.infra.binder.context.segment.select.subquery.engine.SubqueryTableContextEngine;
import org.apache.shardingsphere.infra.binder.context.statement.dml.SelectStatementContext;
import org.apache.shardingsphere.infra.database.core.type.DatabaseType;
import org.apache.shardingsphere.infra.database.core.type.DatabaseTypeRegistry;
import org.apache.shardingsphere.infra.metadata.database.ShardingSphereDatabase;
import org.apache.shardingsphere.infra.metadata.database.schema.model.ShardingSphereSchema;
import org.apache.shardingsphere.sql.parser.statement.core.segment.generic.table.SimpleTableSegment;
import org.apache.shardingsphere.sql.parser.statement.core.segment.generic.table.SubqueryTableSegment;
import org.apache.shardingsphere.sql.parser.statement.core.segment.generic.table.TableNameSegment;
import org.apache.shardingsphere.sql.parser.statement.core.segment.generic.table.TableSegment;
import org.apache.shardingsphere.sql.parser.statement.core.value.identifier.IdentifierValue;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Optional;

/**
 * Tables context.
 */
@Getter
@ToString
public final class TablesContext {
    
    @Getter(AccessLevel.NONE)
    private final Collection<TableSegment> tables = new LinkedList<>();
    
    private final Collection<SimpleTableSegment> simpleTables = new LinkedList<>();
    
    private final Collection<String> tableNames = new CaseInsensitiveSet<>();
    
    private final Collection<String> schemaNames = new CaseInsensitiveSet<>();
    
    private final Collection<String> databaseNames = new CaseInsensitiveSet<>();
    
    @Getter(AccessLevel.NONE)
    private final Map<String, Collection<SubqueryTableContext>> subqueryTables = new HashMap<>();
    
    @SphereEx
    private final Map<String, IdentifierValue> tableNameAliasMap = new HashMap<>();
    
    @SphereEx
    private final boolean containsDual;
    
    public TablesContext(final SimpleTableSegment table) {
        this(null == table ? Collections.emptyList() : Collections.singletonList(table));
    }
    
    public TablesContext(final Collection<SimpleTableSegment> tables) {
        this(tables, Collections.emptyMap());
    }
    
    public TablesContext(final Collection<? extends TableSegment> tables, final Map<Integer, SelectStatementContext> subqueryContexts) {
        // SPEX ADDED: BEGIN
        containsDual = !tables.isEmpty() && isContainsDualTable(tables);
        // SPEX ADDED: END
        if (tables.isEmpty()) {
            return;
        }
        this.tables.addAll(tables);
        for (TableSegment each : tables) {
            if (each instanceof SimpleTableSegment) {
                SimpleTableSegment simpleTableSegment = (SimpleTableSegment) each;
                TableNameSegment tableName = simpleTableSegment.getTableName();
                if (!"DUAL".equalsIgnoreCase(tableName.getIdentifier().getValue())) {
                    simpleTables.add(simpleTableSegment);
                    tableNames.add(tableName.getIdentifier().getValue());
                    // TODO support bind with all statement contains table segment @duanzhengqiang
                    // SPEX CHANGED: BEGIN
                    tableName.getTableBoundInfo().filter(optional -> !Strings.isNullOrEmpty(optional.getOriginalSchema().getValue()))
                            .ifPresent(optional -> schemaNames.add(optional.getOriginalSchema().getValue()));
                    tableName.getTableBoundInfo().filter(optional -> !Strings.isNullOrEmpty(optional.getOriginalDatabase().getValue()))
                            .ifPresent(optional -> databaseNames.add(optional.getOriginalDatabase().getValue()));
                    // SPEX CHANGED: END
                    // SPEX ADDED: BEGIN
                    tableNameAliasMap.put(tableName.getIdentifier().getValue().toLowerCase(), each.getAlias().orElse(simpleTableSegment.getTableName().getIdentifier()));
                    // SPEX ADDED: END
                }
            }
            if (each instanceof SubqueryTableSegment) {
                subqueryTables.putAll(createSubqueryTables(subqueryContexts, (SubqueryTableSegment) each));
            }
        }
    }
    
    @SphereEx
    private boolean isContainsDualTable(final Collection<? extends TableSegment> tables) {
        for (TableSegment each : tables) {
            if (each instanceof SimpleTableSegment && "DUAL".equalsIgnoreCase(((SimpleTableSegment) each).getTableName().getIdentifier().getValue())) {
                return true;
            }
        }
        return false;
    }
    
    private Map<String, Collection<SubqueryTableContext>> createSubqueryTables(final Map<Integer, SelectStatementContext> subqueryContexts, final SubqueryTableSegment subqueryTable) {
        if (!subqueryContexts.containsKey(subqueryTable.getSubquery().getStartIndex())) {
            return Collections.emptyMap();
        }
        SelectStatementContext subqueryContext = subqueryContexts.get(subqueryTable.getSubquery().getStartIndex());
        Map<String, SubqueryTableContext> subqueryTableContexts = new SubqueryTableContextEngine().createSubqueryTableContexts(subqueryContext, subqueryTable.getAliasName().orElse(null));
        Map<String, Collection<SubqueryTableContext>> result = new HashMap<>(subqueryTableContexts.size(), 1F);
        for (SubqueryTableContext each : subqueryTableContexts.values()) {
            if (null != each.getAliasName()) {
                result.computeIfAbsent(each.getAliasName(), unused -> new LinkedList<>()).add(each);
            }
        }
        return result;
    }
    
    /**
     * Get database name.
     *
     * @return database name
     */
    public Optional<String> getDatabaseName() {
        return databaseNames.isEmpty() ? Optional.empty() : Optional.of(databaseNames.iterator().next());
    }
    
    /**
     * Get database names.
     *
     * @return database names
     */
    @SphereEx
    public Collection<String> getDatabaseNames() {
        return databaseNames;
    }
    
    /**
     * Get schema name.
     *
     * @return schema name
     */
    public Optional<String> getSchemaName() {
        return schemaNames.isEmpty() ? Optional.empty() : Optional.of(schemaNames.iterator().next());
    }
    
    /**
     * Get schema.
     *
     * @param database database
     * @param databaseType database type
     * @return schema
     */
    @SphereEx
    public ShardingSphereSchema getSchema(final ShardingSphereDatabase database, final DatabaseType databaseType) {
        String defaultSchema = new DatabaseTypeRegistry(databaseType).getDefaultSchemaName(database.getName());
        return getSchemaName().map(database::getSchema).orElseGet(() -> database.getSchema(defaultSchema));
    }
}
