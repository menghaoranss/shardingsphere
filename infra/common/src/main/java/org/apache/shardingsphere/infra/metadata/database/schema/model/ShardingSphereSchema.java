/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.shardingsphere.infra.metadata.database.schema.model;

import lombok.Getter;
import org.apache.shardingsphere.infra.config.props.temporary.TemporaryConfigurationProperties;
import org.apache.shardingsphere.infra.metadata.database.schema.cache.TableCacheBuilder;
import org.apache.shardingsphere.infra.metadata.database.schema.cache.TableCacheManager;
import org.apache.shardingsphere.infra.metadata.database.schema.cache.TableCacheStatistics;
import org.apache.shardingsphere.infra.metadata.database.schema.util.TableCacheConfigurationUtil;
import org.apache.shardingsphere.infra.metadata.identifier.ShardingSphereIdentifier;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * ShardingSphere schema.
 */
public final class ShardingSphereSchema {

    @Getter
    private final String name;

    private final Map<ShardingSphereIdentifier, ShardingSphereTable> tables;

    private final Map<ShardingSphereIdentifier, ShardingSphereView> views;

    private final TableCacheManager tableCache;

    private final boolean tableCacheEnabled;

    @SuppressWarnings("CollectionWithoutInitialCapacity")
    public ShardingSphereSchema(final String name) {
        this(name, (TemporaryConfigurationProperties) null);
    }

    @SuppressWarnings("CollectionWithoutInitialCapacity")
    public ShardingSphereSchema(final String name, final TemporaryConfigurationProperties props) {
        this.name = name;
        tables = new ConcurrentHashMap<>();
        views = new ConcurrentHashMap<>();

        if (props != null) {
            TableCacheConfigurationUtil configUtil = new TableCacheConfigurationUtil(props);
            this.tableCacheEnabled = configUtil.isTableCacheEnabled();
            this.tableCache = tableCacheEnabled ? TableCacheBuilder.build(name, configUtil.getTableCacheConfiguration()) : null;
        } else {
            this.tableCacheEnabled = false;
            this.tableCache = null;
        }
    }

    @SuppressWarnings("CollectionWithoutInitialCapacity")
    public ShardingSphereSchema(final String name, final Collection<ShardingSphereTable> tables, final Collection<ShardingSphereView> views) {
        this(name, tables, views, (TemporaryConfigurationProperties) null);
    }

    @SuppressWarnings("CollectionWithoutInitialCapacity")
    public ShardingSphereSchema(final String name, final Collection<ShardingSphereTable> tables,
                              final Collection<ShardingSphereView> views, final TemporaryConfigurationProperties props) {
        this.name = name;
        this.tables = new ConcurrentHashMap<>(tables.size(), 1F);
        this.views = new ConcurrentHashMap<>(views.size(), 1F);
        tables.forEach(each -> this.tables.put(new ShardingSphereIdentifier(each.getName()), each));
        views.forEach(each -> this.views.put(new ShardingSphereIdentifier(each.getName()), each));

        if (props != null) {
            TableCacheConfigurationUtil configUtil = new TableCacheConfigurationUtil(props);
            this.tableCacheEnabled = configUtil.isTableCacheEnabled();
            this.tableCache = tableCacheEnabled ? TableCacheBuilder.build(name, configUtil.getTableCacheConfiguration()) : null;
        } else {
            this.tableCacheEnabled = false;
            this.tableCache = null;
        }
    }
    
    /**
     * Get all tables.
     *
     * @return all tables
     */
    public Collection<ShardingSphereTable> getAllTables() {
        return tables.values();
    }
    
    /**
     * Judge whether contains table.
     *
     * @param tableName table name
     * @return contains table or not
     */
    public boolean containsTable(final String tableName) {
        return tables.containsKey(new ShardingSphereIdentifier(tableName));
    }
    
    /**
     * Get table.
     *
     * @param tableName table name
     * @return table
     */
    public ShardingSphereTable getTable(final String tableName) {
        // Prioritize cache if enabled
        return getTableFromCache(tableName);
    }
    
    /**
     * Add table.
     *
     * @param table table
     */
    public void putTable(final ShardingSphereTable table) {
        tables.put(new ShardingSphereIdentifier(table.getName()), table);
    }
    
    /**
     * Remove table.
     *
     * @param tableName table name
     */
    public void removeTable(final String tableName) {
        tables.remove(new ShardingSphereIdentifier(tableName));
    }
    
    /**
     * Get all views.
     *
     * @return all views
     */
    public Collection<ShardingSphereView> getAllViews() {
        return views.values();
    }
    
    /**
     * Judge whether contains view.
     *
     * @param viewName view name
     * @return contains view or not
     */
    public boolean containsView(final String viewName) {
        return views.containsKey(new ShardingSphereIdentifier(viewName));
    }

    /**
     * Get view.
     *
     * @param viewName view name
     * @return view
     */
    public ShardingSphereView getView(final String viewName) {
        return views.get(new ShardingSphereIdentifier(viewName));
    }

    /**
     * Add view.
     *
     * @param view view
     */
    public void putView(final ShardingSphereView view) {
        views.put(new ShardingSphereIdentifier(view.getName()), view);
    }

    /**
     * Remove view.
     *
     * @param viewName view name
     */
    public void removeView(final String viewName) {
        views.remove(new ShardingSphereIdentifier(viewName));
    }

    /**
     * Judge whether contains index.
     *
     * @param tableName table name
     * @param indexName index name
     * @return contains index or not
     */
    public boolean containsIndex(final String tableName, final String indexName) {
        return containsTable(tableName) && getTable(tableName).containsIndex(indexName);
    }

    /**
     * Get visible column names.
     *
     * @param tableName table name
     * @return visible column names
     */
    public List<String> getVisibleColumnNames(final String tableName) {
        return containsTable(tableName) ? getTable(tableName).getVisibleColumns() : Collections.emptyList();
    }

    /**
     * Get visible column and index map.
     *
     * @param tableName table name
     * @return visible column and index map
     */
    public Map<String, Integer> getVisibleColumnAndIndexMap(final String tableName) {
        return containsTable(tableName) ? getTable(tableName).getVisibleColumnAndIndexMap() : Collections.emptyMap();
    }

    /**
     * Get table from cache, load from database if cache miss.
     *
     * @param tableName table name
     * @return table from cache or database
     */
    public ShardingSphereTable getTableFromCache(final String tableName) {
        if (tableCacheEnabled && tableCache != null) {
            ShardingSphereTable cachedTable = tableCache.getTableFromCache(tableName, this::loadTableFromDatabase);
            // Update local storage for backward compatibility
            tables.put(new ShardingSphereIdentifier(tableName), cachedTable);
            return cachedTable;
        }
        // Fallback to existing memory storage when cache is disabled
        return tables.get(new ShardingSphereIdentifier(tableName));
    }

    /**
     * Load table from database.
     *
     * @param tableName table name
     * @return table from database
     */
    private ShardingSphereTable loadTableFromDatabase(final String tableName) {
        // TODO: Implement actual database loading logic for table
        // Currently using empty implementation as placeholder
        return new ShardingSphereTable(tableName, Collections.emptyList(),
                                     Collections.emptyList(), Collections.emptyList());
    }

    /**
     * Invalidate specific table cache.
     *
     * @param tableName table name to invalidate
     */
    public void invalidateTableCache(final String tableName) {
        if (tableCacheEnabled && tableCache != null) {
            tableCache.invalidateTable(tableName);
        }
        // Also remove from local storage
        tables.remove(new ShardingSphereIdentifier(tableName));
    }

    /**
     * Clear all table cache.
     */
    public void clearTableCache() {
        if (tableCacheEnabled && tableCache != null) {
            tableCache.clearAllCache();
        }
        // Also clear local storage
        tables.clear();
    }

    /**
     * Get table cache statistics.
     *
     * @return cache statistics
     */
    public TableCacheStatistics getTableCacheStatistics() {
        if (tableCacheEnabled && tableCache != null) {
            return tableCache.getCacheStats();
        }
        return TableCacheStatistics.empty();
    }

    /**
     * Whether empty schema.
     *
     * @return empty schema or not
     */
    public boolean isEmpty() {
        return tables.isEmpty() && views.isEmpty();
    }
}
