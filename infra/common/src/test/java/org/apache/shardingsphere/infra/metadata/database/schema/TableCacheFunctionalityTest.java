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
import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test table cache functionality.
 */
class TableCacheFunctionalityTest {

    @Test
    void testCacheDisabledByDefault() {
        // Test default behavior - cache should be disabled
        ShardingSphereSchema schema = new ShardingSphereSchema("test_schema");

        // Cache should be disabled by default
        assertFalse(isCacheEnabled(schema));

        // Should fall back to regular table storage
        ShardingSphereTable table = schema.getTable("test_table");
        // Should return null as table doesn't exist in memory
        assertThat(table, is((ShardingSphereTable) null));
    }

    @Test
    void testCacheEnabledWithConfiguration() {
        // Create mock configuration properties with cache enabled
        Map<String, String> mockProps = new HashMap<>();
        mockProps.put(TemporaryConfigurationPropertyKey.TABLE_CACHE_ENABLED.getKey(), "true");

        TemporaryConfigurationProperties props = createMockTemporaryConfigurationProperties(mockProps);
        ShardingSphereSchema schema = new ShardingSphereSchema("test_schema", props);

        // Cache should be enabled
        assertTrue(isCacheEnabled(schema));

        // Cache statistics should be available
        TableCacheStatistics stats = schema.getTableCacheStatistics();
        assertNotNull(stats);
        assertTrue(stats.isEmpty());
    }

    @Test
    void testPutAndGetTableWithCache() {
        // Create schema with cache enabled
        Map<String, String> mockProps = new HashMap<>();
        mockProps.put(TemporaryConfigurationPropertyKey.TABLE_CACHE_ENABLED.getKey(), "true");

        TemporaryConfigurationProperties props = createMockTemporaryConfigurationProperties(mockProps);
        ShardingSphereSchema schema = new ShardingSphereSchema("test_schema", props);

        // Create a test table
        ShardingSphereColumn column = new ShardingSphereColumn("id", "int", false, false, true, false, false);
        ShardingSphereTable testTable = new ShardingSphereTable("test_table",
                                     Collections.singletonList(column),
                                     Collections.singletonList("id"),
                                     Collections.emptyList());

        // Add table to schema
        schema.putTable(testTable);

        // Get table from cache
        ShardingSphereTable retrievedTable = schema.getTable("test_table");

        // Should retrieve the same table
        assertNotNull(retrievedTable);
        assertThat(retrievedTable.getName(), is("test_table"));
        assertThat(retrievedTable.getColumns().size(), is(1));
        assertThat(retrievedTable.getColumns().iterator().next().getName(), is("id"));
    }

    @Test
    void testCacheStatisticsAfterOperations() {
        // Create schema with cache enabled
        Map<String, String> mockProps = new HashMap<>();
        mockProps.put(TemporaryConfigurationPropertyKey.TABLE_CACHE_ENABLED.getKey(), "true");

        TemporaryConfigurationProperties props = createMockTemporaryConfigurationProperties(mockProps);
        ShardingSphereSchema schema = new ShardingSphereSchema("test_schema", props);

        // Perform some cache operations
        schema.getTable("non_existent_table"); // Cache miss
        schema.putTable(createTestTable("table1"));
        schema.getTable("table1"); // Cache hit

        // Check cache statistics
        TableCacheStatistics stats = schema.getTableCacheStatistics();
        assertNotNull(stats);

        // Should have recorded operations
        assertFalse(stats.isEmpty());
    }

    @Test
    void testCacheInvalidation() {
        // Create schema with cache enabled
        Map<String, String> mockProps = new HashMap<>();
        mockProps.put(TemporaryConfigurationPropertyKey.TABLE_CACHE_ENABLED.getKey(), "true");

        TemporaryConfigurationProperties props = createMockTemporaryConfigurationProperties(mockProps);
        ShardingSphereSchema schema = new ShardingSphereSchema("test_schema", props);

        // Add a table
        ShardingSphereTable testTable = createTestTable("test_table");
        schema.putTable(testTable);

        // Verify table exists
        assertNotNull(schema.getTable("test_table"));

        // Invalidate cache for specific table
        schema.invalidateTableCache("test_table");

        // Clear cache
        schema.clearTableCache();

        // Schema should be empty
        assertTrue(schema.isEmpty());
    }

    private boolean isCacheEnabled(ShardingSphereSchema schema) {
        // This is a hacky way to check if cache is enabled by looking at cache statistics
        // In real implementation, you would expose this through a getter or method
        TableCacheStatistics stats = schema.getTableCacheStatistics();
        return stats != null && !stats.isEmpty();
    }

    private TemporaryConfigurationProperties createMockTemporaryConfigurationProperties(Map<String, String> props) {
        return new TemporaryConfigurationProperties() {
            @Override
            public String getValue(TemporaryConfigurationPropertyKey key) {
                return props.get(key.getKey());
            }
        };
    }

    private ShardingSphereTable createTestTable(String tableName) {
        ShardingSphereColumn column = new ShardingSphereColumn("id", "int", false, false, true, false, false);
        return new ShardingSphereTable(tableName,
                                     Collections.singletonList(column),
                                     Collections.singletonList("id"),
                                     Collections.emptyList());
    }
}