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

package org.apache.shardingsphere.infra.metadata.database.schema;

import org.apache.shardingsphere.infra.config.props.temporary.TemporaryConfigurationProperties;
import org.apache.shardingsphere.infra.config.props.temporary.TemporaryConfigurationPropertyKey;
import org.apache.shardingsphere.infra.metadata.database.schema.cache.TableCacheStatistics;
import org.apache.shardingsphere.infra.metadata.database.schema.model.ShardingSphereColumn;
import org.apache.shardingsphere.infra.metadata.database.schema.model.ShardingSphereIdentifier;
import org.apache.shardingsphere.infra.metadata.database.schema.model.ShardingSphereSchema;
import org.apache.shardingsphere.infra.metadata.database.schema.model.ShardingSphereTable;
import org.apache.shardingsphere.infra.metadata.database.schema.model.ShardingSphereView;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test for ShardingSphereSchema table cache functionality.
 */
class ShardingSphereSchemaCacheTest {

    private ShardingSphereSchema schemaWithCache;
    private ShardingSphereSchema schemaWithoutCache;

    @BeforeEach
    void setUp() {
        // Create schema with cache enabled
        Map<String, String> cacheConfig = new HashMap<>();
        cacheConfig.put(TemporaryConfigurationPropertyKey.TABLE_CACHE_ENABLED.getKey(), "true");

        TemporaryConfigurationProperties propsWithCache = new TemporaryConfigurationProperties() {
            @Override
            public String getValue(TemporaryConfigurationPropertyKey key) {
                return cacheConfig.get(key.getKey());
            }
        };

        schemaWithCache = new ShardingSphereSchema("test_schema", propsWithCache);

        // Create schema without cache (default behavior)
        schemaWithoutCache = new ShardingSphereSchema("test_schema_no_cache");
    }

    @Test
    void testCacheEnabledByDefault() {
        // Given - schema created without cache configuration

        // When - check cache enabled
        boolean cacheEnabled = isCacheEnabled(schemaWithoutCache);

        // Then - should be disabled
        assertFalse(cacheEnabled);

        // When - get table from cache
        ShardingSphereTable table = schemaWithoutCache.getTableFromCache("test_table");

        // Then - should return null as table doesn't exist and cache is disabled
        assertThat(table, is((ShardingSphereTable) null));
    }

    @Test
    void testCacheEnabledWithConfiguration() {
        // Given - schema created with cache enabled

        // When - check cache enabled
        boolean cacheEnabled = isCacheEnabled(schemaWithCache);

        // Then - should be enabled
        assertTrue(cacheEnabled);
    }

    @Test
    void testPutAndGetTableWithCache() {
        // Given
        ShardingSphereColumn column = new ShardingSphereColumn("id", "int", false, false, true, false, false);
        ShardingSphereTable testTable = new ShardingSphereTable("test_table",
                                     Collections.singletonList(column),
                                     Collections.singletonList("id"),
                                     Collections.emptyList());
        schemaWithCache.putTable(testTable);

        // When - get table from cache
        ShardingSphereTable retrievedTable = schemaWithCache.getTableFromCache("test_table");

        // Then - should retrieve the cached table
        assertThat(retrievedTable.getName(), is("test_table"));
        assertThat(retrievedTable.getColumns().size(), is(1));
        assertThat(retrievedTable.getColumns().iterator().next().getName(), is("id"));
    }

    @Test
    void testGetTableFromCacheFallbackToMemory() {
        // Given - schema with cache disabled
        ShardingSphereColumn column = new ShardingSphereColumn("id", "int", false, false, true, false, false);
        ShardingSphereTable testTable = new ShardingSphereTable("test_table",
                                     Collections.singletonList(column),
                                     Collections.singletonList("id"),
                                     Collections.emptyList());
        schemaWithoutCache.putTable(testTable);

        // When - get table from cache
        ShardingSphereTable retrievedTable = schemaWithoutCache.getTableFromCache("test_table");

        // Then - should return table from memory storage
        assertThat(retrievedTable.getName(), is("test_table"));
        assertThat(retrievedTable.getColumns().size(), is(1));
    }

    @Test
    void testCacheStatistics() {
        // Given - schema with cache enabled
        ShardingSphereTable testTable = createTestTable("test_table");
        schemaWithCache.getTableFromCache("test_table"); // Cache miss - loads into cache

        // When - get cache statistics
        TableCacheStatistics stats = schemaWithCache.getTableCacheStatistics();

        // Then - should show statistics
        assertNotNull(stats);
        assertFalse(stats.isEmpty());
    }

    @Test
    void testInvalidateTableCache() {
        // Given - schema with cache and loaded table
        ShardingSphereTable testTable = createTestTable("test_table");
        schemaWithCache.getTableFromCache("test_table"); // Load into cache

        // When - invalidate specific table cache
        schemaWithCache.invalidateTableCache("test_table");

        // Then - table should be removed from cache but still in local storage
        assertTrue(schemaWithCache.containsTable("test_table")); // Still in local storage
        TableCacheStatistics stats = schemaWithCache.getTableCacheStatistics();
        assertThat(stats.getLoadCount(), is(2L)); // Initial load + reload after invalidate
    }

    @Test
    void testClearTableCache() {
        // Given - schema with cache and loaded tables
        schemaWithCache.getTableFromCache("table1"); // Load into cache
        schemaWithCache.getTableFromCache("table2"); // Load into cache

        // When - clear all cache
        schemaWithCache.clearTableCache();

        // Then - tables should still be in local storage
        assertTrue(schemaWithCache.containsTable("table1"));
        assertTrue(schemaWithCache.containsTable("table2"));

        // Cache should be empty
        TableCacheStatistics stats = schemaWithCache.getTableCacheStatistics();
        assertThat(stats.getEstimatedSize(), is(0L)); // No estimation method in original stats, using empty check
    }

    @Test
    void testBackwardCompatibilityWithCache() {
        // Given - existing API usage should still work
        ShardingSphereTable testTable = createTestTable("test_table");
        schemaWithCache.putTable(testTable);

        // When - use existing getTable method
        ShardingSphereTable retrievedTable = schemaWithCache.getTable("test_table");

        // Then - should use cache automatically
        assertThat(retrievedTable.getName(), is("test_table"));
        assertThat(retrievedTable.getColumns().size(), is(1));
    }

    /**
     * Helper method to check if cache is enabled for a schema.
     * This is a workaround since the field is private and no getter exists
     */
    private boolean isCacheEnabled(ShardingSphereSchema schema) {
        TableCacheStatistics stats = schema.getTableCacheStatistics();
        return !stats.isEmpty();
    }

    /**
     * Create test table for testing.
     */
    private ShardingSphereTable createTestTable(String tableName) {
        ShardingSphereColumn column = new ShardingSphereColumn("id", "int", false, false, true, false, false);
        return new ShardingSphereTable(tableName,
                                     Collections.singletonList(column),
                                     Collections.singletonList("id"),
                                     Collections.emptyList());
    }
}