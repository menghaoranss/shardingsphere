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

package com.sphereex.dbplusengine.infra.algorithm.cryptographic.rc4;

import com.sphereex.dbplusengine.SphereEx;
import com.sphereex.dbplusengine.SphereEx.Type;
import org.apache.shardingsphere.infra.algorithm.core.exception.AlgorithmInitializationException;
import org.apache.shardingsphere.infra.algorithm.cryptographic.core.CryptographicAlgorithm;
import org.apache.shardingsphere.infra.spi.type.typed.TypedSPILoader;
import org.apache.shardingsphere.test.util.PropertiesBuilder;
import org.apache.shardingsphere.test.util.PropertiesBuilder.Property;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

@SphereEx(Type.COPY)
class RC4CryptographicAlgorithmTest {
    
    private CryptographicAlgorithm cryptographicAlgorithm;
    
    @BeforeEach
    void setUp() {
        cryptographicAlgorithm = TypedSPILoader.getService(CryptographicAlgorithm.class, "RC4", PropertiesBuilder.build(new Property("rc4-key-value", "test-sharding")));
    }
    
    @SphereEx(Type.MODIFY)
    @Test
    void assertEncode() {
        assertThat(cryptographicAlgorithm.encrypt("test"), is("4Tn7lQ=="));
    }
    
    @SphereEx(Type.MODIFY)
    @Test
    void assertEncryptNullValue() {
        assertNull(cryptographicAlgorithm.encrypt(null));
    }
    
    @Test
    void assertKeyIsTooLong() {
        assertThrows(AlgorithmInitializationException.class,
                () -> cryptographicAlgorithm.init(PropertiesBuilder.build(new Property("rc4-key-value", IntStream.range(0, 100).mapToObj(each -> "test").collect(Collectors.joining())))));
    }
    
    @Test
    void assertKeyIsTooShort() {
        assertThrows(AlgorithmInitializationException.class, () -> cryptographicAlgorithm.init(PropertiesBuilder.build(new Property("rc4-key-value", "test"))));
    }
    
    @SphereEx(Type.MODIFY)
    @Test
    void assertDecode() {
        assertThat(cryptographicAlgorithm.decrypt("4Tn7lQ==").toString(), is("test"));
    }
    
    @SphereEx(Type.MODIFY)
    @Test
    void assertDecryptNullValue() {
        assertNull(cryptographicAlgorithm.decrypt(null));
    }
}
