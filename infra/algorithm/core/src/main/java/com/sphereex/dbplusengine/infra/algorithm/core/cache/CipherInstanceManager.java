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

package com.sphereex.dbplusengine.infra.algorithm.core.cache;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.apache.shardingsphere.infra.algorithm.core.config.AlgorithmConfiguration;

import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * Cipher instance manager.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
@Getter
public final class CipherInstanceManager {
    
    private static final ThreadLocal<CipherInstanceManager> CIPHER_INSTANCE_MANAGER_HOLDER = new ThreadLocal<>();
    
    @Setter
    private static volatile boolean cacheEnabled = true;
    
    private final Cache<CipherInstanceCacheKey, CipherInstanceWrapper> caches = Caffeine.newBuilder().maximumSize(100).expireAfterAccess(1, TimeUnit.HOURS).build();
    
    /**
     * Get instance of cipher instance manager.
     *
     * @return instance of cipher instance manager
     */
    public static CipherInstanceManager getInstance() {
        CipherInstanceManager result = CIPHER_INSTANCE_MANAGER_HOLDER.get();
        if (null != result) {
            return result;
        }
        result = new CipherInstanceManager();
        CIPHER_INSTANCE_MANAGER_HOLDER.set(result);
        return result;
    }
    
    /**
     * Get cipher instance.
     *
     * @param key key
     * @param type type
     * @param supplier supplier
     * @param encryptMode encrypt mode
     * @param <T> instance type
     * @return encrypt cipher instance
     */
    public <T> T getCipher(final AlgorithmConfiguration key, final Class<T> type, final Supplier<T> supplier, final boolean encryptMode) {
        return encryptMode ? getEncryptCipher(key, type, supplier) : getDecryptCipher(key, type, supplier);
    }
    
    /**
     * Get encrypt cipher instance.
     *
     * @param key key
     * @param type type
     * @param supplier supplier
     * @param <T> instance type
     * @return encrypt cipher instance
     */
    public <T> T getEncryptCipher(final AlgorithmConfiguration key, final Class<T> type, final Supplier<T> supplier) {
        return getOrCreate(getEncryptCacheKey(key), type, supplier);
    }
    
    private static CipherInstanceCacheKey getEncryptCacheKey(final AlgorithmConfiguration key) {
        return new CipherInstanceCacheKey(key, "encrypt");
    }
    
    /**
     * Get decrypt cipher instance.
     *
     * @param key key
     * @param type type
     * @param supplier supplier
     * @param <T> instance type
     * @return decrypt cipher instance
     */
    public <T> T getDecryptCipher(final AlgorithmConfiguration key, final Class<T> type, final Supplier<T> supplier) {
        return getOrCreate(getDecryptCacheKey(key), type, supplier);
    }
    
    private CipherInstanceCacheKey getDecryptCacheKey(final AlgorithmConfiguration key) {
        return new CipherInstanceCacheKey(key, "decrypt");
    }
    
    private <T> T getOrCreate(final CipherInstanceCacheKey key, final Class<T> type, final Supplier<T> supplier) {
        if (!cacheEnabled) {
            return supplier.get();
        }
        return putIfAbsent(key, type, supplier);
    }
    
    private <T> T putIfAbsent(final CipherInstanceCacheKey key, final Class<T> type, final Supplier<T> supplier) {
        T cache = get(key, type);
        if (null != cache) {
            return cache;
        }
        T result = supplier.get();
        put(key, result);
        return result;
    }
    
    private <T> T get(final CipherInstanceCacheKey key, final Class<T> type) {
        CipherInstanceWrapper wrapper = caches.getIfPresent(key);
        if (null == wrapper) {
            return null;
        }
        Object result = wrapper.getValue();
        if (!type.isInstance(result)) {
            throw new IllegalStateException("Cached value is not of required type [" + type.getName() + "]: " + result);
        }
        return (T) result;
    }
    
    private void put(final CipherInstanceCacheKey key, final Object value) {
        caches.put(key, new CipherInstanceWrapper(value));
    }
}
