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
import org.apache.shardingsphere.infra.metadata.database.schema.model.ShardingSphereSchema;
import org.apache.shardingsphere.infra.metadata.database.schema.model.ShardingSphereTable;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Table cache usage example.
 */
public final class TableCacheUsageExample {

    /**
     * Example of enabling table cache with TemporaryConfigurationProperties.
     */
    public static void enableTableCacheWithTemporaryConfiguration() {
        // Create temporary configuration properties with cache enabled
        Map<String, String> cacheConfig = new HashMap<>();
        cacheConfig.put(TemporaryConfigurationPropertyKey.TABLE_CACHE_ENABLED.getKey(), "true");

        // You can also configure other cache parameters
        cacheConfig.put(TemporaryConfigurationPropertyKey.TABLE_CACHE_INITIAL_CAPACITY.getKey(), "256");
        cacheConfig.put(TemporaryConfigurationPropertyKey.TABLE_CACHE_MAXIMUM_SIZE.getKey(), "1024");
        cacheConfig.put(TemporaryConfigurationPropertyKey.TABLE_CACHE_EXPIRE_AFTER_WRITE.getKey(), "600000");
        cacheConfig.put(TemporaryConfigurationPropertyKey.TABLE_CACHE_EXPIRE_AFTER_ACCESS.getKey(), "900000");
        cacheConfig.put(TemporaryConfigurationPropertyKey.TABLE_CACHE_ENABLE_METRICS.getKey(), "true");
        cacheConfig.put(TemporaryConfigurationPropertyKey.TABLE_CACHE_ENABLE_SOFT_VALUES.getKey(), "true");

        TemporaryConfigurationProperties props = new TemporaryConfigurationProperties() {
            @Override
            public String getValue(final TemporaryConfigurationPropertyKey key) {
                return cacheConfig.get(key.getKey());
            }
        };

        // Create schema with cache enabled
        ShardingSphereSchema schema = new ShardingSphereSchema("example_schema", props);

        // Now table operations will use cache
        ShardingSphereTable table = schema.getTable("example_table");

        // Get cache statistics
        TableCacheStatistics stats = schema.getTableCacheStatistics();
        System.out.println("Cache statistics: " + stats);
    }

    /**
     * Example of using table cache with default configuration (cache disabled).
     */
    public static void useDefaultCacheConfiguration() {
        // Create schema without TemporaryConfigurationProperties (cache disabled by default)
        ShardingSphereSchema schema = new ShardingSphereSchema("example_schema");

        // Table operations will use regular memory storage
        ShardingSphereTable table = schema.getTable("example_table");

        // Cache statistics will be empty
        TableCacheStatistics stats = schema.getTableCacheStatistics();
        System.out.println("Cache statistics: " + stats); // Should show empty stats
    }

    /**
     * Example of cache invalidation.
     */
    public static void demonstrateCacheInvalidation() {
        // Enable cache first
        Map<String, String> cacheConfig = new HashMap<>();
        cacheConfig.put(TemporaryConfigurationPropertyKey.TABLE_CACHE_ENABLED.getKey(), "true");

        TemporaryConfigurationProperties props = new TemporaryConfigurationProperties() {
            @Override
            public String getValue(TemporaryConfigurationPropertyKey key) {
                return cacheConfig.get(key.getKey());
            }
        };

        ShardingSphereSchema schema = new ShardingSphereSchema("example_schema", props);

        // Add a table to cache
        ShardingSphereTable table = createExampleTable("cached_table");
        schema.putTable(table);

        // Retrieve table (will be cached)
        ShardingSphereTable cachedTable = schema.getTable("cached_table");
        System.out.println("Retrieved cached table: " + cachedTable.getName());

        // Invalidate specific table cache
        schema.invalidateTableCache("cached_table");

        // Clear all cache
        schema.clearTableCache();

        // Statistics should reflect operations
        TableCacheStatistics stats = schema.getTableCacheStatistics();
        System.out.println("Final cache statistics: " + stats);
    }

    /**
     * Create example table for demonstration.
     */
    private static ShardingSphereTable createExampleTable(String tableName) {
        return new ShardingSphereTable(tableName,
                                     Collections.emptyList(),
                                     Collections.emptyList(),
                                     Collections.emptyList());
    }

    /**
     * Get configuration string for enabling table cache.
     *
     * @return configuration string
     */
    public static String getEnableCacheConfiguration() {
        return String.join("\n",
                "# Table Cache Configuration Example",
                "# Enable table cache by setting table-cache-enabled=true",
                "# All cache parameters use default values if not specified",
                "",
                "table-cache-enabled=true",
                "table-cache-initial-capacity=256",
                "table-cache-maximum-size=2048",
                "table-cache-expire-after-write=600000",
                "table-cache-expire-after-access=900000",
                "table-cache-enable-metrics=true",
                "table-cache-enable-soft-values=true");
    }
}
