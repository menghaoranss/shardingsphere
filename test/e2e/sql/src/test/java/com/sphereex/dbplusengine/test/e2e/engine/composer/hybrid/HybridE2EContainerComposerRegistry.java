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

package com.sphereex.dbplusengine.test.e2e.engine.composer.hybrid;

import lombok.AccessLevel;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import org.apache.shardingsphere.infra.database.core.type.DatabaseType;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Hybrid E2E container composer registry.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class HybridE2EContainerComposerRegistry {
    
    private static final HybridE2EContainerComposerRegistry INSTANCE = new HybridE2EContainerComposerRegistry();
    
    private final Map<Key, HybridE2EContainerComposer> cached = new ConcurrentHashMap<>();
    
    private final Map<Key, AtomicInteger> referenceCount = new ConcurrentHashMap<>();
    
    /**
     * Get instance of {@link HybridE2EContainerComposerRegistry}.
     *
     * @return instance of {@link HybridE2EContainerComposerRegistry}
     */
    public static HybridE2EContainerComposerRegistry getInstance() {
        return INSTANCE;
    }
    
    /**
     * Get {@link HybridE2EContainerComposer}.
     *
     * @param scenario scenario
     * @param databaseType database type
     * @param mode mode
     * @return {@link HybridE2EContainerComposer}
     */
    public HybridE2EContainerComposer getContainerComposer(final String scenario, final DatabaseType databaseType, final String mode) {
        return cached.computeIfAbsent(new Key(scenario, databaseType, mode), this::newHybridE2EContainerComposer);
    }
    
    @SneakyThrows({SQLException.class, IOException.class})
    private HybridE2EContainerComposer newHybridE2EContainerComposer(final Key key) {
        return new HybridE2EContainerComposer(key.scenario, key.databaseType, key.mode);
    }
    
    /**
     * Retain.
     *
     * @param scenario scenario
     * @param databaseType database type
     * @param mode mode
     */
    public void retain(final String scenario, final DatabaseType databaseType, final String mode) {
        Key key = new Key(scenario, databaseType, mode);
        referenceCount.computeIfAbsent(key, unused -> new AtomicInteger()).incrementAndGet();
    }
    
    /**
     * Release.
     *
     * @param scenario scenario
     * @param databaseType database type
     * @param mode mode
     */
    public void release(final String scenario, final DatabaseType databaseType, final String mode) {
        Key key = new Key(scenario, databaseType, mode);
        if (referenceCount.get(key).decrementAndGet() > 0) {
            return;
        }
        cached.computeIfPresent(key, (unused, value) -> {
            value.close();
            return null;
        });
    }
    
    @RequiredArgsConstructor
    @EqualsAndHashCode
    private static class Key {
        
        private final String scenario;
        
        private final DatabaseType databaseType;
        
        private final String mode;
    }
}
