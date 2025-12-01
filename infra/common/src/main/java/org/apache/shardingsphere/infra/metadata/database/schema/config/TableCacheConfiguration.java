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

package org.apache.shardingsphere.infra.metadata.database.schema.config;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

/**
 * Table cache configuration.
 */
@Getter
@RequiredArgsConstructor
public final class TableCacheConfiguration {

    private final int initialCapacity;

    private final long maximumSize;

    private final long expireAfterWriteMillis;

    private final long expireAfterAccessMillis;

    private final boolean enableMetrics;

    private final boolean softValues;

    /**
     * Default constructor.
     */
    public TableCacheConfiguration() {
        this(128, 1024, 300000, 600000, false, true);
    }
}
