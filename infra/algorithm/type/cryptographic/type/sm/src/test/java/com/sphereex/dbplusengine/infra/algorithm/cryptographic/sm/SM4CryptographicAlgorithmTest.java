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

package com.sphereex.dbplusengine.infra.algorithm.cryptographic.sm;

import com.sphereex.dbplusengine.SphereEx;
import com.sphereex.dbplusengine.SphereEx.Type;
import org.apache.shardingsphere.infra.algorithm.core.exception.AlgorithmInitializationException;
import org.apache.shardingsphere.infra.algorithm.cryptographic.core.CryptographicAlgorithm;
import org.apache.shardingsphere.infra.spi.type.typed.TypedSPILoader;
import org.apache.shardingsphere.test.util.PropertiesBuilder;
import org.apache.shardingsphere.test.util.PropertiesBuilder.Property;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class SM4CryptographicAlgorithmTest {
    
    @Test
    void assertInitWithoutKey() {
        assertThrows(AlgorithmInitializationException.class,
                () -> TypedSPILoader.getService(CryptographicAlgorithm.class, "SM4", PropertiesBuilder.build(new Property("sm4-mode", "ECB"), new Property("sm4-padding", "PKCS5Padding"))));
    }
    
    @SphereEx(Type.MODIFY)
    @Test
    void assertEncryptNullValue() {
        CryptographicAlgorithm algorithm = TypedSPILoader.getService(CryptographicAlgorithm.class, "SM4", createECBProperties());
        assertNull(algorithm.encrypt(null));
    }
    
    @SphereEx(Type.MODIFY)
    @Test
    void assertEncryptWithECBMode() {
        CryptographicAlgorithm algorithm = TypedSPILoader.getService(CryptographicAlgorithm.class, "SM4", createECBProperties());
        assertThat(algorithm.encrypt("test"), is("028654f2ca4f575dee9e1faae85dadde"));
    }
    
    @SphereEx
    @Test
    void assertInitWithGCMNNotNoPaddingMode() {
        assertThrows(AlgorithmInitializationException.class,
                () -> TypedSPILoader.getService(CryptographicAlgorithm.class, "SM4", PropertiesBuilder.build(new Property("sm4-mode", "GCM"), new Property("sm4-padding", "PKCS5Padding"))));
    }
    
    @SphereEx(Type.MODIFY)
    @Test
    void assertDecryptNullValue() {
        CryptographicAlgorithm algorithm = TypedSPILoader.getService(CryptographicAlgorithm.class, "SM4", createECBProperties());
        assertNull(algorithm.decrypt(null));
    }
    
    @SphereEx(Type.MODIFY)
    @Test
    void assertDecryptWithECBMode() {
        CryptographicAlgorithm algorithm = TypedSPILoader.getService(CryptographicAlgorithm.class, "SM4", createECBProperties());
        assertThat(algorithm.decrypt("028654f2ca4f575dee9e1faae85dadde").toString(), is("test"));
    }
    
    private Properties createECBProperties() {
        return PropertiesBuilder.build(new Property("sm4-key", "4D744E003D713D054E7E407C350E447E"), new Property("sm4-mode", "ECB"), new Property("sm4-padding", "PKCS5Padding"));
    }
    
    @SphereEx(Type.MODIFY)
    @Test
    void assertEncryptWithCBCMode() {
        CryptographicAlgorithm algorithm = TypedSPILoader.getService(CryptographicAlgorithm.class, "SM4", createCBCProperties());
        assertThat(algorithm.encrypt("test"), is("dca2127b57ba8cac36a0914e0208dc11"));
    }
    
    @SphereEx(Type.MODIFY)
    @Test
    void assertDecrypt() {
        CryptographicAlgorithm algorithm = TypedSPILoader.getService(CryptographicAlgorithm.class, "SM4", createCBCProperties());
        assertThat(algorithm.decrypt("dca2127b57ba8cac36a0914e0208dc11").toString(), is("test"));
    }
    
    private Properties createCBCProperties() {
        return PropertiesBuilder.build(
                new Property("sm4-key", "f201326119911788cFd30575b81059ac"), new Property("sm4-iv", "e166c3391294E69cc4c620f594fe00d7"),
                new Property("sm4-mode", "CBC"), new Property("sm4-padding", "PKCS7Padding"));
    }
    
    @SphereEx(Type.MODIFY)
    @Test
    void assertEncryptAndDecryptWithOFBMode() {
        CryptographicAlgorithm algorithm = TypedSPILoader.getService(CryptographicAlgorithm.class, "SM4", createOFBProperties());
        Object encryptedText = algorithm.encrypt("test");
        Object decryptedText = algorithm.decrypt(encryptedText);
        assertThat(decryptedText, is("test"));
    }
    
    private Properties createOFBProperties() {
        return PropertiesBuilder.build(
                new Property("sm4-key", "f201326119911788cFd30575b81059ac"), new Property("sm4-iv", "e166c3391294E69cc4c620f594fe00d7"),
                new Property("sm4-mode", "OFB"), new Property("sm4-padding", "PKCS7Padding"));
    }
    
    @SphereEx(Type.MODIFY)
    @Test
    void assertEncryptAndDecryptWithCFBMode() {
        CryptographicAlgorithm algorithm = TypedSPILoader.getService(CryptographicAlgorithm.class, "SM4", createCFBProperties());
        Object encryptedText = algorithm.encrypt("test");
        assertThat(encryptedText, is("d6d763bafd42dc2a584c542a01571afc"));
        assertThat(algorithm.decrypt(encryptedText), is("test"));
    }
    
    private Properties createCFBProperties() {
        return PropertiesBuilder.build(
                new Property("sm4-key", "f201326119911788cFd30575b81059ac"), new Property("sm4-iv", "e166c3391294E69cc4c620f594fe00d7"),
                new Property("sm4-mode", "CFB"), new Property("sm4-padding", "PKCS7Padding"));
    }
    
    @SphereEx(Type.MODIFY)
    @Test
    void assertEncryptWithGCMMode() {
        CryptographicAlgorithm algorithm = TypedSPILoader.getService(CryptographicAlgorithm.class, "SM4", createGCMProperties());
        Object encryptedText = algorithm.encrypt("test");
        Object decryptedText = algorithm.decrypt(encryptedText);
        assertThat(decryptedText, is("test"));
    }
    
    @SphereEx(Type.MODIFY)
    @Test
    void assertEncryptWithGCMModeTimes() {
        CryptographicAlgorithm algorithm = TypedSPILoader.getService(CryptographicAlgorithm.class, "SM4", createGCMProperties());
        for (int i = 0; i < 5; i++) {
            Object encryptedText = algorithm.encrypt("test");
            Object decryptedText = algorithm.decrypt(encryptedText);
            assertThat(decryptedText, is("test"));
        }
    }
    
    private Properties createGCMProperties() {
        return PropertiesBuilder.build(
                new Property("sm4-key", "f201326119911788cFd30575b81059ac"), new Property("sm4-iv", "e166c3391294E69cc4c620f594fe00d7"),
                new Property("sm4-mode", "GCM"), new Property("sm4-padding", "NoPadding"));
    }
    
    @SphereEx(Type.MODIFY)
    @Test
    void assertEncryptWithCCMMode() {
        CryptographicAlgorithm algorithm = TypedSPILoader.getService(CryptographicAlgorithm.class, "SM4", createCCMProperties());
        Object encryptedText = algorithm.encrypt("test");
        Object decryptedText = algorithm.decrypt(encryptedText);
        assertThat(decryptedText, is("test"));
    }
    
    private Properties createCCMProperties() {
        return PropertiesBuilder.build(
                new Property("sm4-key", "f201326119911788cFd30575b81059ac"), new Property("sm4-iv", "3132333435363738"),
                new Property("sm4-mode", "CCM"), new Property("sm4-padding", "NoPadding"));
    }
}
