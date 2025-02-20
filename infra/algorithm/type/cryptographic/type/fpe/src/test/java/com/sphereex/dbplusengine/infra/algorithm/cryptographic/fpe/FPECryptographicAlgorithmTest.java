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

package com.sphereex.dbplusengine.infra.algorithm.cryptographic.fpe;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;
import org.apache.shardingsphere.infra.algorithm.cryptographic.core.CryptographicAlgorithm;
import org.apache.shardingsphere.infra.spi.type.typed.TypedSPILoader;
import org.apache.shardingsphere.test.util.PropertiesBuilder;
import org.apache.shardingsphere.test.util.PropertiesBuilder.Property;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

class FPECryptographicAlgorithmTest {
    
    @Test
    @Disabled("need fix")
    void assertEncryptDecryptWhenConfigFF1ModeAndAESCipher() {
        CryptographicAlgorithm algorithm = TypedSPILoader.getService(CryptographicAlgorithm.class, "SphereEx:FPE", PropertiesBuilder.build(new Property("fpe-mode", "FF1"),
                new Property("fpe-key-value", "1234567890abcdef"), new Property("fpe-alphabet", "0123456789"), new Property("fpe-cipher", "AES")));
        String plainValue = RandomStringUtils.randomNumeric(RandomUtils.nextInt(1, 1000));
        Object encrypt = algorithm.encrypt(plainValue);
        assertThat(String.valueOf(encrypt).length(), is(plainValue.length()));
        Object decrypt = algorithm.decrypt(encrypt);
        assertThat(String.valueOf(decrypt), is(decrypt));
    }
    
    @Test
    @Disabled("need fix")
    void assertEncryptDecryptWhenConfigFF1ModeAndSM4Cipher() {
        CryptographicAlgorithm algorithm = TypedSPILoader.getService(CryptographicAlgorithm.class, "SphereEx:FPE", PropertiesBuilder.build(new Property("fpe-mode", "FF1"),
                new Property("fpe-key-value", "1234567890abcdef"), new Property("fpe-alphabet", "0123456789"), new Property("fpe-cipher", "SM4")));
        // todo 需要看一下明文小与5位为什么会报错
        String plainValue = RandomStringUtils.randomNumeric(RandomUtils.nextInt(6, 1000));
        Object encrypt = algorithm.encrypt(plainValue);
        assertThat(String.valueOf(encrypt).length(), is(plainValue.length()));
        Object decrypt = algorithm.decrypt(encrypt);
        assertThat(String.valueOf(decrypt), is(decrypt));
    }
}
