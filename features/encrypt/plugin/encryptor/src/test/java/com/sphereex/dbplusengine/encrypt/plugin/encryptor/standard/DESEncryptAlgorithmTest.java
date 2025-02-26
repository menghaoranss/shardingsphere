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

package com.sphereex.dbplusengine.encrypt.plugin.encryptor.standard;

import com.google.common.base.Strings;
import com.sphereex.dbplusengine.SphereEx;
import com.sphereex.dbplusengine.SphereEx.Type;
import com.sphereex.dbplusengine.encrypt.context.EncryptContext;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.shardingsphere.encrypt.spi.EncryptAlgorithm;
import org.apache.shardingsphere.infra.algorithm.core.context.AlgorithmSQLContext;
import org.apache.shardingsphere.infra.algorithm.core.exception.AlgorithmInitializationException;
import org.apache.shardingsphere.infra.spi.type.typed.TypedSPILoader;
import org.apache.shardingsphere.test.util.PropertiesBuilder;
import org.apache.shardingsphere.test.util.PropertiesBuilder.Property;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;

class DESEncryptAlgorithmTest {
    
    private static final String ALGORITHM_NAME = "SphereEx:DES";
    
    private EncryptAlgorithm encryptAlgorithm;
    
    @BeforeEach
    void setUp() {
        encryptAlgorithm = TypedSPILoader.getService(EncryptAlgorithm.class, ALGORITHM_NAME, PropertiesBuilder.build(new Property("des-key-value", "test")));
    }
    
    @Test
    void assertDefaultDigestAlgorithm() throws NoSuchAlgorithmException {
        try (MockedStatic<DigestUtils> mockedDigestUtils = mockStatic(DigestUtils.class)) {
            mockedDigestUtils.when(() -> DigestUtils.getDigest("SHA-256")).thenReturn(MessageDigest.getInstance("SHA-256"));
            TypedSPILoader.getService(EncryptAlgorithm.class, ALGORITHM_NAME, PropertiesBuilder.build(new Property("des-key-value", "test")));
            mockedDigestUtils.verify(() -> DigestUtils.getDigest("SHA-256"));
        }
    }
    
    @Test
    void assertSHA512DigestAlgorithm() throws NoSuchAlgorithmException {
        try (MockedStatic<DigestUtils> mockedDigestUtils = mockStatic(DigestUtils.class)) {
            mockedDigestUtils.when(() -> DigestUtils.getDigest("SHA-512")).thenReturn(MessageDigest.getInstance("SHA-512"));
            TypedSPILoader.getService(EncryptAlgorithm.class, ALGORITHM_NAME, PropertiesBuilder.build(new Property("des-key-value", "test"), new Property("digest-algorithm-name", "SHA-512")));
            mockedDigestUtils.verify(() -> DigestUtils.getDigest("SHA-512"));
        }
    }
    
    @Test
    void assertCreateNewInstanceWithoutDESKey() {
        assertThrows(AlgorithmInitializationException.class, () -> TypedSPILoader.getService(EncryptAlgorithm.class, ALGORITHM_NAME));
    }
    
    @Test
    void assertCreateNewInstanceWithEmptyDESKey() {
        assertThrows(AlgorithmInitializationException.class, () -> encryptAlgorithm.init(PropertiesBuilder.build(new Property("des-key-value", ""))));
    }
    
    @SphereEx(Type.MODIFY)
    @Test
    void assertEncrypt() {
        Object actual = encryptAlgorithm.encrypt("test", mock(AlgorithmSQLContext.class), mock(EncryptContext.class));
        assertThat(actual, is("wrqisAqNZmc="));
    }
    
    @SphereEx(Type.MODIFY)
    @Test
    void assertEncryptNullValue() {
        assertNull(encryptAlgorithm.encrypt(null, mock(AlgorithmSQLContext.class), mock(EncryptContext.class)));
    }
    
    @SphereEx(Type.MODIFY)
    @Test
    void assertDecrypt() {
        Object actual = encryptAlgorithm.decrypt("wrqisAqNZmc=", mock(AlgorithmSQLContext.class), mock(EncryptContext.class));
        assertThat(actual.toString(), is("test"));
    }
    
    @SphereEx(Type.MODIFY)
    @Test
    void assertDecryptNullValue() {
        assertNull(encryptAlgorithm.decrypt(null, mock(AlgorithmSQLContext.class), mock(EncryptContext.class)));
    }
    
    @Test
    void assertExpansibility() {
        String plainValue = Strings.repeat("漢", 100);
        int actualCipherCharLength = encryptAlgorithm.encrypt(plainValue, mock(AlgorithmSQLContext.class), mock(EncryptContext.class)).toString().length();
        int expectedMaxCipherCharLength = encryptAlgorithm.getMetaData().getExpansibility().calculate(plainValue.length(), 4);
        assertThat(actualCipherCharLength, is(408));
        assertThat(expectedMaxCipherCharLength, is(544));
        assertThat(actualCipherCharLength, lessThanOrEqualTo(expectedMaxCipherCharLength));
        plainValue = Strings.repeat("漢", 200);
        actualCipherCharLength = encryptAlgorithm.encrypt(plainValue, mock(AlgorithmSQLContext.class), mock(EncryptContext.class)).toString().length();
        expectedMaxCipherCharLength = encryptAlgorithm.getMetaData().getExpansibility().calculate(plainValue.length(), 4);
        assertThat(actualCipherCharLength, is(812));
        assertThat(expectedMaxCipherCharLength, is(1080));
        assertThat(actualCipherCharLength, lessThanOrEqualTo(expectedMaxCipherCharLength));
    }
}
