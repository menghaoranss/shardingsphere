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
import com.github.benmanes.caffeine.cache.LoadingCache;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.apache.shardingsphere.infra.metadata.database.schema.config.TableCacheConfiguration;
import org.apache.shardingsphere.infra.metadata.database.schema.loader.TableCacheLoader;
import org.apache.shardingsphere.infra.metadata.database.schema.model.ShardingSphereTable;

import java.time.Duration;

/**
 * Table cache builder.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class TableCacheBuilder {

    /**
     * Build table cache manager.
     *
     * @param schemaName schema name
     * @param config table cache configuration
     * @return table cache manager
     */
    public static TableCacheManager build(final String schemaName, final TableCacheConfiguration config) {
        LoadingCache<String, ShardingSphereTable> cache = Caffeine.newBuilder()
                .initialCapacity(config.getInitialCapacity())
                .maximumSize(config.getMaximumSize())
                .expireAfterWrite(Duration.ofMillis(config.getExpireAfterWriteMillis()))
                .expireAfterAccess(Duration.ofMillis(config.getExpireAfterAccessMillis()))
                //.recordStats(config.isEnableMetrics())
                //.softValues(config.isSoftValues())
                .build(new TableCacheLoader());

        return new TableCacheManager(cache, schemaName, config);
    }
}
