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

package org.apache.shardingsphere.infra.config.props.temporary;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.apache.shardingsphere.infra.props.TypedPropertyKey;

import java.util.Arrays;
import java.util.Collection;
import java.util.stream.Collectors;

/**
 * Temporary typed property key of configuration.
 */
@RequiredArgsConstructor
@Getter
public enum TemporaryConfigurationPropertyKey implements TypedPropertyKey {
    
    /**
     * Proxy meta data collector enabled.
     */
    PROXY_META_DATA_COLLECTOR_ENABLED("proxy-meta-data-collector-enabled", String.valueOf(Boolean.FALSE), boolean.class, false),
    
    /**
     * System schema metadata assembly enabled.
     */
    SYSTEM_SCHEMA_METADATA_ASSEMBLY_ENABLED("system-schema-metadata-assembly-enabled", String.valueOf(Boolean.TRUE), boolean.class, true),
    
    /**
     * Proxy meta data collector cron.
     */
    PROXY_META_DATA_COLLECTOR_CRON("proxy-meta-data-collector-cron", "0 0/1 * * * ?", String.class, false),

    /**
     * Table cache enabled.
     */
    TABLE_CACHE_ENABLED("table-cache-enabled", String.valueOf(Boolean.FALSE), boolean.class, false),

    /**
     * Table cache initial capacity.
     */
    TABLE_CACHE_INITIAL_CAPACITY("table-cache-initial-capacity", "128", int.class, false),

    /**
     * Table cache maximum size.
     */
    TABLE_CACHE_MAXIMUM_SIZE("table-cache-maximum-size", "1024", int.class, false),

    /**
     * Table cache expire after write in milliseconds.
     */
    TABLE_CACHE_EXPIRE_AFTER_WRITE("table-cache-expire-after-write", "300000", long.class, false),

    /**
     * Table cache expire after access in milliseconds.
     */
    TABLE_CACHE_EXPIRE_AFTER_ACCESS("table-cache-expire-after-access", "600000", long.class, false),

    /**
     * Table cache enable metrics.
     */
    TABLE_CACHE_ENABLE_METRICS("table-cache-enable-metrics", String.valueOf(Boolean.FALSE), boolean.class, false),

    /**
     * Table cache enable soft values.
     */
    TABLE_CACHE_ENABLE_SOFT_VALUES("table-cache-enable-soft-values", String.valueOf(Boolean.TRUE), boolean.class, false);
    
    private final String key;
    
    private final String defaultValue;
    
    private final Class<?> type;
    
    private final boolean rebootRequired;
    
    /**
     * Get internal property key names.
     *
     * @return collection of internal key names
     */
    public static Collection<String> getKeyNames() {
        return Arrays.stream(values()).map(TemporaryConfigurationPropertyKey::name).collect(Collectors.toList());
    }
}
