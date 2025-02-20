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

package com.sphereex.dbplusengine.encrypt.algorithm.standard;

import com.google.common.base.Strings;
import com.sphereex.dbplusengine.SphereEx;
import com.sphereex.dbplusengine.SphereEx.Type;
import com.sphereex.dbplusengine.encrypt.context.EncryptContext;
import org.apache.shardingsphere.encrypt.spi.EncryptAlgorithm;
import org.apache.shardingsphere.infra.algorithm.core.context.AlgorithmSQLContext;
import org.apache.shardingsphere.infra.algorithm.core.exception.AlgorithmInitializationException;
import org.apache.shardingsphere.infra.spi.type.typed.TypedSPILoader;
import org.apache.shardingsphere.test.util.PropertiesBuilder;
import org.apache.shardingsphere.test.util.PropertiesBuilder.Property;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

@SphereEx(Type.COPY)
class RC4EncryptAlgorithmTest {
    
    private EncryptAlgorithm encryptAlgorithm;
    
    @BeforeEach
    void setUp() {
        encryptAlgorithm = TypedSPILoader.getService(EncryptAlgorithm.class, "RC4", PropertiesBuilder.build(new Property("rc4-key-value", "test-sharding")));
    }
    
    @SphereEx(Type.MODIFY)
    @Test
    void assertEncode() {
        assertThat(encryptAlgorithm.encrypt("test", mock(AlgorithmSQLContext.class), mock(EncryptContext.class)), is("4Tn7lQ=="));
    }
    
    @SphereEx(Type.MODIFY)
    @Test
    void assertEncryptNullValue() {
        assertNull(encryptAlgorithm.encrypt(null, mock(AlgorithmSQLContext.class), mock(EncryptContext.class)));
    }
    
    @Test
    void assertKeyIsTooLong() {
        assertThrows(AlgorithmInitializationException.class,
                () -> encryptAlgorithm.init(PropertiesBuilder.build(new Property("rc4-key-value", IntStream.range(0, 100).mapToObj(each -> "test").collect(Collectors.joining())))));
    }
    
    @Test
    void assertKeyIsTooShort() {
        assertThrows(AlgorithmInitializationException.class, () -> encryptAlgorithm.init(PropertiesBuilder.build(new Property("rc4-key-value", "test"))));
    }
    
    @SphereEx(Type.MODIFY)
    @Test
    void assertDecode() {
        assertThat(encryptAlgorithm.decrypt("4Tn7lQ==", mock(AlgorithmSQLContext.class), mock(EncryptContext.class)).toString(), is("test"));
    }
    
    @SphereEx(Type.MODIFY)
    @Test
    void assertDecryptNullValue() {
        assertNull(encryptAlgorithm.decrypt(null, mock(AlgorithmSQLContext.class), mock(EncryptContext.class)));
    }
    
    @SphereEx
    @Test
    void assertExpansibility() {
        String plainValue = Strings.repeat("漢", 100);
        assertThat(encryptAlgorithm.encrypt(plainValue, mock(AlgorithmSQLContext.class), mock(EncryptContext.class)).toString().length(),
                lessThanOrEqualTo(encryptAlgorithm.getMetaData().getExpansibility().calculate(plainValue.length(), 4)));
    }
}
