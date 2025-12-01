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

import com.github.benmanes.caffeine.cache.stats.CacheStats;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

/**
 * Table cache statistics.
 */
@Getter
@RequiredArgsConstructor
public final class TableCacheStatistics {

    private final long hitCount;

    private final long missCount;

    private final double hitRate;

    private final long evictionCount;

    private final long loadCount;

    private final double averageLoadPenalty;
    
    public TableCacheStatistics(final CacheStats cacheStats) {
        hitCount = cacheStats.hitCount();
        missCount = cacheStats.missCount();
        hitRate = (hitCount + missCount) == 0 ? 0.0 : (double) hitCount / (hitCount + missCount);
        evictionCount = cacheStats.evictionCount();
        loadCount = cacheStats.loadCount();
        averageLoadPenalty = cacheStats.averageLoadPenalty();
    }

    /**
     * Create empty statistics when cache is disabled.
     *
     * @return empty statistics
     */
    public static TableCacheStatistics empty() {
        return new TableCacheStatistics(0L, 0L, 0.0, 0L, 0L, 0.0);
    }

    @Override
    public String toString() {
        return String.format("TableCacheStatistics{hitCount=%d, missCount=%d, hitRate=%.2f%%, evictionCount=%d, loadCount=%d, averageLoadPenalty=%.2fms}",
                hitCount, missCount, hitRate * 100, evictionCount, loadCount, averageLoadPenalty / 1_000_000.0);
    }
}
