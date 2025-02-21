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

package com.sphereex.dbplusengine.infra.algorithm.messagedigest.sm3;

import com.google.common.base.Strings;
import com.sphereex.dbplusengine.infra.expansibility.ExpansibilityProvider;
import org.apache.shardingsphere.infra.algorithm.core.exception.AlgorithmInitializationException;
import org.apache.shardingsphere.infra.algorithm.messagedigest.core.MessageDigestAlgorithm;
import org.apache.shardingsphere.infra.spi.type.typed.TypedSPILoader;
import org.apache.shardingsphere.test.util.PropertiesBuilder;
import org.apache.shardingsphere.test.util.PropertiesBuilder.Property;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class SM3HMACMessageDigestAlgorithmTest {
    
    private MessageDigestAlgorithm digestAlgorithm;
    
    @BeforeEach
    void setUp() {
        digestAlgorithm = TypedSPILoader.getService(MessageDigestAlgorithm.class, "HMAC_SM3", PropertiesBuilder.build(new Property("hmac-sm3-salt", "12345678")));
    }
    
    @Test
    void assertInitWithoutSalt() {
        assertThrows(AlgorithmInitializationException.class, () -> digestAlgorithm.init(new Properties()));
    }
    
    @Test
    void assertDigest() {
        Object actual = digestAlgorithm.digest("Hello, World!");
        assertThat(actual, is("f4072ab8b5e62609af1de2de14b2b3df4908fa7ddd250293a8791511a505e2d7"));
    }
    
    @Test
    void assertDigestWithNullPlaintext() {
        assertNull(digestAlgorithm.digest(null));
    }
    
    @Test
    void assertExpansibility() {
        String plainValue = Strings.repeat("漢", 100);
        ExpansibilityProvider expansibilityProvider = (ExpansibilityProvider) digestAlgorithm;
        int actualCipherCharLength = digestAlgorithm.digest(plainValue).length();
        int expectedMaxCipherCharLength = expansibilityProvider.calculate(plainValue.length(), 4);
        assertThat(actualCipherCharLength, is(64));
        assertThat(expectedMaxCipherCharLength, is(64));
        assertThat(actualCipherCharLength, is(expectedMaxCipherCharLength));
        plainValue = Strings.repeat("漢", 200);
        actualCipherCharLength = digestAlgorithm.digest(plainValue).length();
        expectedMaxCipherCharLength = expansibilityProvider.calculate(plainValue.length(), 4);
        assertThat(actualCipherCharLength, is(64));
        assertThat(expectedMaxCipherCharLength, is(64));
        assertThat(actualCipherCharLength, is(expectedMaxCipherCharLength));
    }
}
