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

import com.github.benmanes.caffeine.cache.LoadingCache;
import lombok.RequiredArgsConstructor;
import org.apache.shardingsphere.infra.metadata.database.schema.config.TableCacheConfiguration;
import org.apache.shardingsphere.infra.metadata.database.schema.model.ShardingSphereTable;

import java.util.function.Function;

/**
 * Table cache manager.
 */
@RequiredArgsConstructor
public final class TableCacheManager {

    private final LoadingCache<String, ShardingSphereTable> cache;

    private final String schemaName;

    private final TableCacheConfiguration configuration;

    /**
     * Get table from cache, load from database if cache miss.
     *
     * @param tableName table name
     * @param databaseLoader database loader function
     * @return table from cache or database
     */
    public ShardingSphereTable getTableFromCache(final String tableName, final Function<String, ShardingSphereTable> databaseLoader) {
        return cache.get(tableName, databaseLoader);
    }

    /**
     * Invalidate specific table cache.
     *
     * @param tableName table name to invalidate
     */
    public void invalidateTable(final String tableName) {
        cache.invalidate(tableName);
    }

    /**
     * Clear all table cache.
     */
    public void clearAllCache() {
        cache.invalidateAll();
    }

    /**
     * Get cache statistics.
     *
     * @return cache statistics
     */
    public TableCacheStatistics getCacheStats() {
        return new TableCacheStatistics(cache.stats());
    }

    /**
     * Get estimated cache size.
     *
     * @return estimated size
     */
    public long estimatedSize() {
        return cache.estimatedSize();
    }
}
