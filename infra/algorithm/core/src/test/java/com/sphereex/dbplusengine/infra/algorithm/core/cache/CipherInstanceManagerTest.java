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

import org.apache.shardingsphere.infra.algorithm.core.config.AlgorithmConfiguration;
import org.apache.shardingsphere.test.util.PropertiesBuilder;
import org.apache.shardingsphere.test.util.PropertiesBuilder.Property;
import org.junit.jupiter.api.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CipherInstanceManagerTest {
    
    @Test
    void assertEncryptCipherCache() {
        CipherInstanceManager instance = CipherInstanceManager.getInstance();
        CipherInstanceManager.setCacheEnabled(true);
        assertEncryptCipherCache(instance, new AlgorithmConfiguration("AES", PropertiesBuilder.build(new Property("aes-key-value", "12345678-1"))), "testValue");
        assertEncryptCipherCache(instance, new AlgorithmConfiguration("AES", PropertiesBuilder.build(new Property("aes-key-value", "12345678-2"))), "testValue2");
        assertEncryptCipherCache(instance, new AlgorithmConfiguration("AES", PropertiesBuilder.build(new Property("aes-key-value", "12345678-2"))), "testValue2");
        assertEncryptCipherCache(instance, new AlgorithmConfiguration("AES", PropertiesBuilder.build(new Property("aes-key-value", "12345678-3"))), "testValue3");
        instance.getCaches().invalidateAll();
    }
    
    private void assertEncryptCipherCache(final CipherInstanceManager instance, final AlgorithmConfiguration key, final String value) {
        String cacheValue = instance.getEncryptCipher(key, String.class, () -> value);
        assertThat(cacheValue, is(value));
        assertTrue(instance.getCaches().asMap().containsKey(new CipherInstanceCacheKey(key, "encrypt")));
        CipherInstanceWrapper valueWrapper = instance.getCaches().getIfPresent(new CipherInstanceCacheKey(key, "encrypt"));
        assertThat(valueWrapper.getValue(), is(value));
    }
    
    @Test
    void assertEncryptCipherWithoutCache() {
        CipherInstanceManager instance = CipherInstanceManager.getInstance();
        CipherInstanceManager.setCacheEnabled(false);
        assertEncryptCipherWithoutCache(instance, new AlgorithmConfiguration("AES", PropertiesBuilder.build(new Property("aes-key-value", "12345678-1"))), "testValue");
        assertEncryptCipherWithoutCache(instance, new AlgorithmConfiguration("AES", PropertiesBuilder.build(new Property("aes-key-value", "12345678-2"))), "testValue2");
        assertEncryptCipherWithoutCache(instance, new AlgorithmConfiguration("AES", PropertiesBuilder.build(new Property("aes-key-value", "12345678-2"))), "testValue2");
        assertEncryptCipherWithoutCache(instance, new AlgorithmConfiguration("AES", PropertiesBuilder.build(new Property("aes-key-value", "12345678-3"))), "testValue3");
        instance.getCaches().invalidateAll();
    }
    
    private void assertEncryptCipherWithoutCache(final CipherInstanceManager instance, final AlgorithmConfiguration key, final String value) {
        String cipherValue = instance.getEncryptCipher(key, String.class, () -> value);
        assertThat(cipherValue, is(value));
        assertTrue(instance.getCaches().asMap().isEmpty());
    }
    
    @Test
    void assertModifyCacheEnabled() {
        CipherInstanceManager instance = CipherInstanceManager.getInstance();
        CipherInstanceManager.setCacheEnabled(true);
        assertEncryptCipherCache(instance, new AlgorithmConfiguration("AES", PropertiesBuilder.build(new Property("aes-key-value", "12345678-1"))), "testValue");
        assertEncryptCipherCache(instance, new AlgorithmConfiguration("AES", PropertiesBuilder.build(new Property("aes-key-value", "12345678-2"))), "testValue2");
        CipherInstanceManager.setCacheEnabled(false);
        assertThat(instance.getCaches().asMap().size(), is(2));
        instance.getCaches().invalidateAll();
        assertEncryptCipherWithoutCache(instance, new AlgorithmConfiguration("AES", PropertiesBuilder.build(new Property("aes-key-value", "12345678-1"))), "testValue");
        assertDecryptCipherWithoutCache(instance, new AlgorithmConfiguration("AES", PropertiesBuilder.build(new Property("aes-key-value", "12345678-2"))), "testValue2");
        CipherInstanceManager.setCacheEnabled(true);
        assertEncryptCipherCache(instance, new AlgorithmConfiguration("AES", PropertiesBuilder.build(new Property("aes-key-value", "12345678-1"))), "testValue");
        assertEncryptCipherCache(instance, new AlgorithmConfiguration("AES", PropertiesBuilder.build(new Property("aes-key-value", "12345678-2"))), "testValue2");
        assertThat(instance.getCaches().asMap().size(), is(2));
        instance.getCaches().invalidateAll();
    }
    
    @Test
    void assertDecryptCipherCache() {
        CipherInstanceManager instance = CipherInstanceManager.getInstance();
        CipherInstanceManager.setCacheEnabled(true);
        assertDecryptCipherCache(instance, new AlgorithmConfiguration("AES", PropertiesBuilder.build(new Property("aes-key-value", "12345678-1"))), "testValue");
        assertDecryptCipherCache(instance, new AlgorithmConfiguration("AES", PropertiesBuilder.build(new Property("aes-key-value", "12345678-2"))), "testValue2");
        assertDecryptCipherCache(instance, new AlgorithmConfiguration("AES", PropertiesBuilder.build(new Property("aes-key-value", "12345678-2"))), "testValue2");
        assertDecryptCipherCache(instance, new AlgorithmConfiguration("AES", PropertiesBuilder.build(new Property("aes-key-value", "12345678-3"))), "testValue3");
        instance.getCaches().invalidateAll();
    }
    
    private void assertDecryptCipherCache(final CipherInstanceManager instance, final AlgorithmConfiguration key, final String value) {
        String cacheValue = instance.getDecryptCipher(key, String.class, () -> value);
        assertThat(cacheValue, is(value));
        assertTrue(instance.getCaches().asMap().containsKey(new CipherInstanceCacheKey(key, "decrypt")));
        CipherInstanceWrapper valueWrapper = instance.getCaches().getIfPresent(new CipherInstanceCacheKey(key, "decrypt"));
        assertThat(valueWrapper.getValue(), is(value));
    }
    
    @Test
    void assertDecryptCipherWithoutCache() {
        CipherInstanceManager instance = CipherInstanceManager.getInstance();
        CipherInstanceManager.setCacheEnabled(false);
        assertDecryptCipherWithoutCache(instance, new AlgorithmConfiguration("AES", PropertiesBuilder.build(new Property("aes-key-value", "12345678-1"))), "testValue");
        assertDecryptCipherWithoutCache(instance, new AlgorithmConfiguration("AES", PropertiesBuilder.build(new Property("aes-key-value", "12345678-2"))), "testValue2");
        assertDecryptCipherWithoutCache(instance, new AlgorithmConfiguration("AES", PropertiesBuilder.build(new Property("aes-key-value", "12345678-3"))), "testValue3");
        instance.getCaches().invalidateAll();
    }
    
    private void assertDecryptCipherWithoutCache(final CipherInstanceManager instance, final AlgorithmConfiguration key, final String value) {
        String cipherValue = instance.getDecryptCipher(key, String.class, () -> value);
        assertThat(cipherValue, is(value));
        assertTrue(instance.getCaches().asMap().isEmpty());
    }
}
