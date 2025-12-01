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

package org.apache.shardingsphere.infra.metadata.database.schema.util;

import lombok.RequiredArgsConstructor;
import org.apache.shardingsphere.infra.config.props.temporary.TemporaryConfigurationProperties;
import org.apache.shardingsphere.infra.config.props.temporary.TemporaryConfigurationPropertyKey;
import org.apache.shardingsphere.infra.metadata.database.schema.config.TableCacheConfiguration;

/**
 * Table cache configuration utility.
 */
@RequiredArgsConstructor
public final class TableCacheConfigurationUtil {

    private final TemporaryConfigurationProperties props;

    /**
     * Check if table cache is enabled.
     *
     * @return true if table cache is enabled, false otherwise
     */
    public boolean isTableCacheEnabled() {
        return Boolean.parseBoolean(
                props.getValue(TemporaryConfigurationPropertyKey.TABLE_CACHE_ENABLED)
        );
    }

    /**
     * Get table cache configuration from global properties.
     *
     * @return table cache configuration
     */
    public TableCacheConfiguration getTableCacheConfiguration() {
        // Read all cache parameters from global configuration
        int initialCapacity = Integer.parseInt(props.getValue(TemporaryConfigurationPropertyKey.TABLE_CACHE_INITIAL_CAPACITY));
        long maximumSize = Long.parseLong(props.getValue(TemporaryConfigurationPropertyKey.TABLE_CACHE_MAXIMUM_SIZE));
        long expireAfterWriteMillis = Long.parseLong(props.getValue(TemporaryConfigurationPropertyKey.TABLE_CACHE_EXPIRE_AFTER_WRITE));
        long expireAfterAccessMillis = Long.parseLong(props.getValue(TemporaryConfigurationPropertyKey.TABLE_CACHE_EXPIRE_AFTER_ACCESS));
        boolean enableMetrics = Boolean.parseBoolean(props.getValue(TemporaryConfigurationPropertyKey.TABLE_CACHE_ENABLE_METRICS));
        boolean softValues = Boolean.parseBoolean(props.getValue(TemporaryConfigurationPropertyKey.TABLE_CACHE_ENABLE_SOFT_VALUES));

        return new TableCacheConfiguration(initialCapacity, maximumSize, expireAfterWriteMillis, expireAfterAccessMillis, enableMetrics, softValues);
    }

    /**
     * Get default table cache configuration.
     *
     * @return default table cache configuration
     */
    public static TableCacheConfiguration getDefaultTableCacheConfiguration() {
        return new TableCacheConfiguration();
    }
}
