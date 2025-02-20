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

package com.sphereex.dbplusengine.infra.algorithm.cryptographic.aes;

import org.apache.shardingsphere.infra.algorithm.cryptographic.core.CryptographicAlgorithm;
import org.apache.shardingsphere.infra.spi.type.typed.TypedSPILoader;
import org.apache.shardingsphere.test.util.PropertiesBuilder;
import org.apache.shardingsphere.test.util.PropertiesBuilder.Property;
import org.junit.jupiter.api.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

class MariaDBAESCryptographicAlgorithmTest {
    
    @Test
    void assertMariaDBEncrypt() {
        CryptographicAlgorithm mariaDBAlgorithm = TypedSPILoader.getService(CryptographicAlgorithm.class, "AES",
                PropertiesBuilder.build(new Property("db-compatible-mode", "MariaDB"), new Property("aes-key-value", "test")));
        assertEncryptAndDecrypt(mariaDBAlgorithm, "test", "h72QOIWUO+SKTmirY7Dsag==");
        assertEncryptAndDecrypt(mariaDBAlgorithm, "testLongLongLongLongLongLongText", "XezHvcnO8wdvgShK7zfkwkjv1E68q56tAl02B5abhD2g21cScTY7szV2iz5gX1TC");
    }
    
    @Test
    void assertMariaDBEncryptWithHexEncode() {
        CryptographicAlgorithm mariaDBAlgorithm = TypedSPILoader.getService(CryptographicAlgorithm.class, "AES",
                PropertiesBuilder.build(new Property("db-compatible-mode", "MariaDB"), new Property("aes-key-value", "test"), new Property("aes-encoder", "HEX")));
        assertEncryptAndDecrypt(mariaDBAlgorithm, "test", "87BD903885943BE48A4E68AB63B0EC6A");
        assertEncryptAndDecrypt(mariaDBAlgorithm, "testLongLongLongLongLongLongText", "5DECC7BDC9CEF3076F81284AEF37E4C248EFD44EBCAB9EAD025D3607969B843DA0DB571271363BB335768B3E605F54C2");
    }
    
    @Test
    void assertMariaDBEncryptECBMode() {
        CryptographicAlgorithm mariaDBAlgorithm = TypedSPILoader.getService(CryptographicAlgorithm.class, "AES",
                PropertiesBuilder.build(new Property("db-compatible-mode", "MariaDB"), new Property("aes-key-value", "test"), new Property("aes-mode", "ECB")));
        assertEncryptAndDecrypt(mariaDBAlgorithm, "test", "h72QOIWUO+SKTmirY7Dsag==");
        assertEncryptAndDecrypt(mariaDBAlgorithm, "testLongLongLongLongLongLongText", "XezHvcnO8wdvgShK7zfkwkjv1E68q56tAl02B5abhD2g21cScTY7szV2iz5gX1TC");
        mariaDBAlgorithm = TypedSPILoader.getService(CryptographicAlgorithm.class, "AES",
                PropertiesBuilder.build(new Property("db-compatible-mode", "MariaDB"), new Property("aes-key-value", "test"), new Property("aes-mode", "ECB")));
        assertEncryptAndDecrypt(mariaDBAlgorithm, "test", "h72QOIWUO+SKTmirY7Dsag==");
        assertEncryptAndDecrypt(mariaDBAlgorithm, "testLongLongLongLongLongLongText", "XezHvcnO8wdvgShK7zfkwkjv1E68q56tAl02B5abhD2g21cScTY7szV2iz5gX1TC");
        CryptographicAlgorithm mariaDBAlgorithmWithBitLength = TypedSPILoader.getService(CryptographicAlgorithm.class, "AES",
                PropertiesBuilder.build(new Property("db-compatible-mode", "MariaDB"), new Property("aes-key-value", "test"), new Property("aes-mode", "ECB"),
                        new Property("aes-iv", "1234567890123456789101112"),
                        new Property("aes-key-bit-length", "256")));
        assertEncryptAndDecrypt(mariaDBAlgorithmWithBitLength, "test", "33zefe1wMVR3XvkzkVBo9Q==");
        assertEncryptAndDecrypt(mariaDBAlgorithmWithBitLength, "testLongLongLongLongLongLongText", "zBFHqvW6pAR6act13haFGs8AbNxppUYlNkUl4jgY2DlMRa3XTxT8JTn9P2HTOObF");
    }
    
    @Test
    void assertMariaDBEncryptCBCMode() {
        CryptographicAlgorithm mariaDBAlgorithm = TypedSPILoader.getService(CryptographicAlgorithm.class, "AES",
                PropertiesBuilder.build(new Property("db-compatible-mode", "MariaDB"), new Property("aes-key-value", "test"), new Property("aes-mode", "CBC"),
                        new Property("aes-iv", "1234567890123456")));
        assertEncryptAndDecrypt(mariaDBAlgorithm, "test", "Pxhu56gOh2ejMBaP3XAgbw==");
        assertEncryptAndDecrypt(mariaDBAlgorithm, "testLongLongLongLongLongLongText", "1uq71jRFWqRu9qXKs6O2mI/4newPn+dSMl1hE+bbniW847aqSaYw43I+Nr1qM6q8");
        mariaDBAlgorithm = TypedSPILoader.getService(CryptographicAlgorithm.class, "AES",
                PropertiesBuilder.build(new Property("db-compatible-mode", "MariaDB"), new Property("aes-key-value", "test"), new Property("aes-mode", "CBC"),
                        new Property("aes-iv", "1234567890123456789101112")));
        assertEncryptAndDecrypt(mariaDBAlgorithm, "test", "Pxhu56gOh2ejMBaP3XAgbw==");
        assertEncryptAndDecrypt(mariaDBAlgorithm, "testLongLongLongLongLongLongText", "1uq71jRFWqRu9qXKs6O2mI/4newPn+dSMl1hE+bbniW847aqSaYw43I+Nr1qM6q8");
        CryptographicAlgorithm mariaDBAlgorithmWithBitLength = TypedSPILoader.getService(CryptographicAlgorithm.class, "AES",
                PropertiesBuilder.build(new Property("db-compatible-mode", "MariaDB"), new Property("aes-key-value", "test"), new Property("aes-mode", "CBC"),
                        new Property("aes-iv", "1234567890123456789101112"),
                        new Property("aes-key-bit-length", "256")));
        assertEncryptAndDecrypt(mariaDBAlgorithmWithBitLength, "test", "DMYTovkGsN8wQf9JPzGdfw==");
        assertEncryptAndDecrypt(mariaDBAlgorithmWithBitLength, "testLongLongLongLongLongLongText", "U7PTO/qwz4/8yS+GZGTG34CEJ9qtO1aWWQT7toUrbMv5neMImrd3n1ttZJCM859K");
    }
    
    @Test
    void assertMariaDBEncryptCTRMode() {
        CryptographicAlgorithm mariaDBAlgorithm = TypedSPILoader.getService(CryptographicAlgorithm.class, "AES",
                PropertiesBuilder.build(new Property("db-compatible-mode", "MariaDB"), new Property("aes-key-value", "test"), new Property("aes-mode", "CTR"),
                        new Property("aes-iv", "1234567890123456")));
        assertEncryptAndDecrypt(mariaDBAlgorithm, "test", "Xyix5UI1/7AbtmtJX5AgWQ==");
        assertEncryptAndDecrypt(mariaDBAlgorithm, "testLongLongLongLongLongLongText", "Xyix5QJWndtb1QkiH/NCMlUCPtWR7K7LNI3N8tY+kNDPc90qoAgQ7zq8paF4O6dJ");
        mariaDBAlgorithm = TypedSPILoader.getService(CryptographicAlgorithm.class, "AES",
                PropertiesBuilder.build(new Property("db-compatible-mode", "MariaDB"), new Property("aes-key-value", "test"), new Property("aes-mode", "CTR"),
                        new Property("aes-iv", "1234567890123456789101112")));
        assertEncryptAndDecrypt(mariaDBAlgorithm, "test", "Xyix5UI1/7AbtmtJX5AgWQ==");
        assertEncryptAndDecrypt(mariaDBAlgorithm, "testLongLongLongLongLongLongText", "Xyix5QJWndtb1QkiH/NCMlUCPtWR7K7LNI3N8tY+kNDPc90qoAgQ7zq8paF4O6dJ");
    }
    
    private void assertEncryptAndDecrypt(final CryptographicAlgorithm cryptographicAlgorithm, final String plainValue, final String encryptedValue) {
        String encryptedText = cryptographicAlgorithm.encrypt(plainValue).toString();
        assertThat(encryptedText, is(encryptedValue));
        assertThat(cryptographicAlgorithm.decrypt(encryptedText), is(plainValue));
    }
}
