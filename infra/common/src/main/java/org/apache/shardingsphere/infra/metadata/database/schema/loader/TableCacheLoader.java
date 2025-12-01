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

package org.apache.shardingsphere.infra.metadata.database.schema.loader;

import com.github.benmanes.caffeine.cache.CacheLoader;
import org.apache.shardingsphere.infra.metadata.database.schema.model.ShardingSphereTable;

import java.util.Collections;

/**
 * Table cache loader.
 */
public final class TableCacheLoader implements CacheLoader<String, ShardingSphereTable> {

    @Override
    public ShardingSphereTable load(final String tableName) {
        // TODO Implement actual database loading logic for table
        // Currently using empty implementation as placeholder
        return createEmptyTable(tableName);
    }

    /**
     * Create empty table for testing.
     *
     * @param tableName table name
     * @return empty table
     */
    private ShardingSphereTable createEmptyTable(final String tableName) {
        return new ShardingSphereTable(tableName, Collections.emptyList(),
                                     Collections.emptyList(), Collections.emptyList());
    }
}
