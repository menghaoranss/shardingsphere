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

package com.sphereex.dbplusengine.encrypt.algorithm.assisted;

import com.google.common.base.Strings;
import com.sphereex.dbplusengine.encrypt.context.EncryptContext;
import org.apache.shardingsphere.encrypt.spi.EncryptAlgorithm;
import org.apache.shardingsphere.infra.algorithm.core.context.AlgorithmSQLContext;
import org.apache.shardingsphere.infra.spi.type.typed.TypedSPILoader;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

class SHAAssistedEncryptAlgorithmTest {
    
    private EncryptAlgorithm encryptAlgorithm;
    
    @BeforeEach
    void setUp() {
        encryptAlgorithm = TypedSPILoader.getService(EncryptAlgorithm.class, "SphereEx:SHA", new Properties());
    }
    
    @Test
    void assertEncrypt() {
        Object actual = encryptAlgorithm.encrypt("test1234", mock(AlgorithmSQLContext.class), mock(EncryptContext.class));
        assertThat(actual, is("k36NX7tIvUlJU2zWW401xCa4DS+DDFwwjizexCKuIkQ="));
    }
    
    @Test
    void assertEncryptNullValue() {
        assertNull(encryptAlgorithm.encrypt(null, mock(AlgorithmSQLContext.class), mock(EncryptContext.class)));
    }
    
    @Test
    void assertDecrypt() {
        String cipherValue = "k36NX7tIvUlJU2zWW401xCa4DS+DDFwwjizexCKuIkQ=";
        assertThrows(UnsupportedOperationException.class, () -> encryptAlgorithm.decrypt(cipherValue, mock(AlgorithmSQLContext.class), mock(EncryptContext.class)));
    }
    
    @Test
    void assertDecryptWithNullValue() {
        assertThrows(UnsupportedOperationException.class, () -> encryptAlgorithm.decrypt(null, mock(AlgorithmSQLContext.class), mock(EncryptContext.class)));
    }
    
    @Test
    void assertExpansibility() {
        String plainValue = Strings.repeat("漢", 100);
        int actualCipherCharLength = encryptAlgorithm.encrypt(plainValue, mock(AlgorithmSQLContext.class), mock(EncryptContext.class)).toString().length();
        int expectedMaxCipherCharLength = encryptAlgorithm.getMetaData().getExpansibility().calculate(plainValue.length(), 4);
        assertThat(actualCipherCharLength, is(44));
        assertThat(expectedMaxCipherCharLength, is(44));
        assertThat(actualCipherCharLength, is(expectedMaxCipherCharLength));
        plainValue = Strings.repeat("漢", 200);
        actualCipherCharLength = encryptAlgorithm.encrypt(plainValue, mock(AlgorithmSQLContext.class), mock(EncryptContext.class)).toString().length();
        expectedMaxCipherCharLength = encryptAlgorithm.getMetaData().getExpansibility().calculate(plainValue.length(), 4);
        assertThat(actualCipherCharLength, is(44));
        assertThat(expectedMaxCipherCharLength, is(44));
        assertThat(actualCipherCharLength, is(expectedMaxCipherCharLength));
    }
}
