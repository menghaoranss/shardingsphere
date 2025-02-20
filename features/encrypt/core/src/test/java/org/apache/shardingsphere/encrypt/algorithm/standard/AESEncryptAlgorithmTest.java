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

package org.apache.shardingsphere.encrypt.algorithm.standard;

import com.google.common.base.Strings;
import com.sphereex.dbplusengine.SphereEx;
import com.sphereex.dbplusengine.SphereEx.Type;
import com.sphereex.dbplusengine.encrypt.context.EncryptContext;
import org.apache.shardingsphere.encrypt.spi.EncryptAlgorithm;
import org.apache.shardingsphere.infra.algorithm.core.config.AlgorithmConfiguration;
import org.apache.shardingsphere.infra.algorithm.core.context.AlgorithmSQLContext;
import org.apache.shardingsphere.infra.algorithm.core.exception.AlgorithmInitializationException;
import org.apache.shardingsphere.infra.spi.type.typed.TypedSPILoader;
import org.apache.shardingsphere.test.util.PropertiesBuilder;
import org.apache.shardingsphere.test.util.PropertiesBuilder.Property;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

class AESEncryptAlgorithmTest {
    
    private EncryptAlgorithm encryptAlgorithm;
    
    @BeforeEach
    void setUp() {
        encryptAlgorithm = TypedSPILoader.getService(EncryptAlgorithm.class, "AES", PropertiesBuilder.build(new Property("aes-key-value", "test"), new Property("digest-algorithm-name", "SHA-1")));
    }
    
    @SphereEx(Type.MODIFY)
    @Test
    void assertEncrypt() {
        assertThat(encryptAlgorithm.encrypt("test", mock(AlgorithmSQLContext.class), mock(EncryptContext.class)), is("dSpPiyENQGDUXMKFMJPGWA=="));
    }
    
    @SphereEx(Type.MODIFY)
    @Test
    void assertEncryptNullValue() {
        assertNull(encryptAlgorithm.encrypt(null, mock(AlgorithmSQLContext.class), mock(EncryptContext.class)));
    }
    
    @SphereEx(Type.MODIFY)
    @Test
    void assertDecrypt() {
        assertThat(encryptAlgorithm.decrypt("dSpPiyENQGDUXMKFMJPGWA==", mock(AlgorithmSQLContext.class), mock(EncryptContext.class)), is("test"));
    }
    
    @Test
    void assertToConfiguration() {
        AlgorithmConfiguration actual = encryptAlgorithm.toConfiguration();
        assertThat(actual.getType(), is("AES"));
        assertThat(actual.getProps().size(), is(2));
        assertThat(actual.getProps().getProperty("aes-key-value"), is("test"));
        assertThat(actual.getProps().getProperty("digest-algorithm-name"), is("SHA-1"));
    }
    
    @SphereEx
    @Test
    void assertCreateNewInstanceWithWrongAesKeyBitLength() {
        assertThrows(AlgorithmInitializationException.class, () -> TypedSPILoader.getService(EncryptAlgorithm.class, "AES",
                PropertiesBuilder.build(new Property("aes-key-value", "test"), new Property("digest-algorithm-name", "SHA-1"), new Property("aes-key-bit-length", "11"))));
    }
    
    @SphereEx
    @Test
    void assertEncryptWithAesKeyBitLengthOf128() {
        assertEncryptWithAesKeyBitLength("dSpPiyENQGDUXMKFMJPGWA==", "128");
    }
    
    @SphereEx
    @Test
    void assertEncryptWithAesKeyBitLengthOf192() {
        assertEncryptWithAesKeyBitLength("6VWdvY0UXthqgJq4HTMoCQ==", "192");
    }
    
    @SphereEx
    @Test
    void assertEncryptWithAesKeyBitLengthOf256() {
        assertEncryptWithAesKeyBitLength("w+O8+FCyWa/dZywjGjcjuQ==", "256");
    }
    
    @SphereEx
    private void assertEncryptWithAesKeyBitLength(final String expectedEncryptedText, final String aesKeyBitLength) {
        EncryptAlgorithm algorithm = TypedSPILoader.getService(EncryptAlgorithm.class, "AES",
                PropertiesBuilder.build(new Property("aes-key-value", "test"), new Property("digest-algorithm-name", "SHA-1"), new Property("aes-key-bit-length", aesKeyBitLength)));
        Object actualEncryptedText = algorithm.encrypt("test", mock(AlgorithmSQLContext.class), mock(EncryptContext.class));
        assertThat(actualEncryptedText.toString(), is(expectedEncryptedText));
        Object actualDecryptedText = algorithm.decrypt(expectedEncryptedText, mock(AlgorithmSQLContext.class), mock(EncryptContext.class));
        assertThat(actualDecryptedText.toString(), is("test"));
    }
    
    @SphereEx
    @Test
    void assertExpansibility() {
        String plainValue = Strings.repeat("漢", 100);
        int actualCipherCharLength = encryptAlgorithm.encrypt(plainValue, mock(AlgorithmSQLContext.class), mock(EncryptContext.class)).toString().length();
        int expectedMaxCipherCharLength = encryptAlgorithm.getMetaData().getExpansibility().calculate(plainValue.length(), 4);
        assertThat(actualCipherCharLength, is(408));
        assertThat(expectedMaxCipherCharLength, is(556));
        assertThat(actualCipherCharLength, lessThanOrEqualTo(expectedMaxCipherCharLength));
        plainValue = Strings.repeat("漢", 200);
        actualCipherCharLength = encryptAlgorithm.encrypt(plainValue, mock(AlgorithmSQLContext.class), mock(EncryptContext.class)).toString().length();
        expectedMaxCipherCharLength = encryptAlgorithm.getMetaData().getExpansibility().calculate(plainValue.length(), 4);
        assertThat(actualCipherCharLength, is(812));
        assertThat(expectedMaxCipherCharLength, is(1088));
        assertThat(actualCipherCharLength, lessThanOrEqualTo(expectedMaxCipherCharLength));
    }
}
