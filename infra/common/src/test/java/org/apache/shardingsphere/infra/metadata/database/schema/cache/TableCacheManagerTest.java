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

package org.apache.shardingsphere.infra.metadata.database.schema.cache;

import com.github.benmanes.caffeine.cache.Caffeine;
import org.apache.shardingsphere.infra.metadata.database.schema.config.TableCacheConfiguration;
import org.apache.shardingsphere.infra.metadata.database.schema.model.ShardingSphereColumn;
import org.apache.shardingsphere.infra.metadata.database.schema.model.ShardingSphereTable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test for TableCacheManager.
 */
class TableCacheManagerTest {

    private TableCacheManager cacheManager;

    @BeforeEach
    void setUp() {
        TableCacheConfiguration configuration = new TableCacheConfiguration();
        cacheManager = new TableCacheManager(
            Caffeine.newBuilder()
                .initialCapacity(configuration.getInitialCapacity())
                .maximumSize(configuration.getMaximumSize())
                .recordStats()
                .build(tableName -> loadFromDatabase(tableName)),
            "test_schema",
            configuration
        );
    }

    @Test
    void testGetTableFromCacheWithCacheHit() {
        // Given
        String tableName = "test_table";
        ShardingSphereTable expectedTable = createTestTable(tableName);

        // When - get table for the first time (cache miss)
        ShardingSphereTable firstResult = cacheManager.getTableFromCache(tableName, this::loadFromDatabase);

        // Then - should load from database
        assertThat(firstResult.getName(), is(tableName));
        assertThat(firstResult.getAllColumns().size(), is(1));

        // When - get table again (cache hit)
        ShardingSphereTable secondResult = cacheManager.getTableFromCache(tableName, this::loadFromDatabase);

        // Then - should return cached table (same instance)
        assertThat(secondResult.getName(), is(expectedTable.getName()));
        assertThat(secondResult.getAllColumns().size(), is(1));

        // Cache statistics should show hits
        TableCacheStatistics stats = cacheManager.getCacheStats();
        assertThat(stats.getHitCount(), is(1L));
        assertThat(stats.getMissCount(), is(1L));
        assertThat(stats.getHitRate(), is(0.5)); // 1 hit out of 2 requests = 50%
    }

    @Test
    void testGetTableFromCacheWithCacheMiss() {
        // Given
        String tableName = "miss_table";

        // When - get table (cache miss)
        ShardingSphereTable result = cacheManager.getTableFromCache(tableName, this::loadFromDatabase);

        // Then - should load from database
        assertThat(result.getName(), is(tableName));
        assertThat(result.getAllColumns().size(), is(1));

        // Cache statistics should show miss
        TableCacheStatistics stats = cacheManager.getCacheStats();
        assertThat(stats.getHitCount(), is(0L));
        assertThat(stats.getMissCount(), is(1L));
        assertThat(stats.getHitRate(), is(0.0));
    }

    @Test
    void testGetTableFromCacheWithException() {
        // Given
        String tableName = "error_table";

        // When - database loading throws exception
        assertThrows(RuntimeException.class, () -> cacheManager.getTableFromCache(tableName, name -> {
            throw new RuntimeException("Database error");
        }));
    }

    @Test
    void testInvalidateTable() {
        // Given
        String tableName = "invalidate_table";
        cacheManager.getTableFromCache(tableName, this::loadFromDatabase); // Load into cache

        // When - invalidate table
        cacheManager.invalidateTable(tableName);

        // Then - get table again should be cache miss
        ShardingSphereTable result = cacheManager.getTableFromCache(tableName, this::loadFromDatabase);

        // Then - should reload from database
        assertThat(result.getName(), is(tableName));

        // Cache statistics should show new miss
        TableCacheStatistics stats = cacheManager.getCacheStats();
        assertThat(stats.getMissCount(), is(1L));
        assertThat(stats.getHitCount(), is(0L));
    }

    @Test
    void testClearAllCache() {
        // Given
        cacheManager.getTableFromCache("table1", this::loadFromDatabase);
        cacheManager.getTableFromCache("table2", this::loadFromDatabase);

        // When - clear all cache
        cacheManager.clearAllCache();

        // Then - next access should be cache miss
        ShardingSphereTable result1 = cacheManager.getTableFromCache("table1", this::loadFromDatabase);
        ShardingSphereTable result2 = cacheManager.getTableFromCache("table2", this::loadFromDatabase);

        // Then - should reload from database
        assertThat(result1.getName(), is("table1"));
        assertThat(result2.getName(), is("table2"));
    }

    @Test
    void testCacheStatistics() {
        // Given
        String tableName = "stats_table";
        cacheManager.getTableFromCache(tableName, this::loadFromDatabase); // Load
        cacheManager.getTableFromCache(tableName, this::loadFromDatabase); // Hit
        cacheManager.getTableFromCache("other_table", this::loadFromDatabase); // Miss

        // When - get cache statistics
        TableCacheStatistics stats = cacheManager.getCacheStats();

        // Then - should record operations correctly
        assertTrue(stats.getLoadCount() >= 2); // At least 2 loads
        assertThat(stats.getHitCount(), is(1L)); // 1 hit
        assertThat(stats.getMissCount(), is(2L)); // 2 misses
    }

    @Test
    void testEstimatedSize() {
        // Given
        String tableName = "size_table";
        cacheManager.getTableFromCache(tableName, this::loadFromDatabase);

        // When - get estimated size
        long size = cacheManager.estimatedSize();

        // Then - should return 1
        assertThat(size, is(1L));
    }

    /**
     * Create test table for testing.
     *
     * @param tableName table name
     * @return test table
     */
    private ShardingSphereTable createTestTable(String tableName) {
        ShardingSphereColumn column = new ShardingSphereColumn("id", 4, true, false, true, true, false, true);
        return new ShardingSphereTable(tableName,
                                     Collections.singletonList(column),
                                     Collections.emptyList(),
                                     Collections.emptyList());
    }

    /**
     * Mock database loading for testing.
     *
     * @param tableName table name
     * @return table from "database"
     */
    private ShardingSphereTable loadFromDatabase(String tableName) {
        return createTestTable(tableName);
    }
}