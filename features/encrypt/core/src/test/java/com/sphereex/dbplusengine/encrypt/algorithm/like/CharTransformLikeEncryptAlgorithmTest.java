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

package com.sphereex.dbplusengine.encrypt.algorithm.like;

import com.google.common.base.Strings;
import com.sphereex.dbplusengine.SphereEx;
import com.sphereex.dbplusengine.SphereEx.Type;
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
import static org.mockito.Mockito.mock;

class CharTransformLikeEncryptAlgorithmTest {
    
    private EncryptAlgorithm likeEncryptAlgorithm;
    
    @BeforeEach
    void setUp() {
        likeEncryptAlgorithm = TypedSPILoader.getService(EncryptAlgorithm.class, "SphereEx:CHAR_TRANSFORM_LIKE", new Properties());
    }
    
    @SphereEx(Type.MODIFY)
    @Test
    void assertEncrypt() {
        assertThat(likeEncryptAlgorithm.encrypt("1234567890%abcdefghijklmnopqrstuvwxyz%ABCDEFGHIJKLMNOPQRSTVWXYZ", mock(AlgorithmSQLContext.class), mock(EncryptContext.class)),
                is("$2&! #7-,0%KHINOLMBC@AFGDEZ[XY^u\\]RSP%khinolmbc`afgdez{xy~|}rsp"));
        assertThat(likeEncryptAlgorithm.encrypt("中国", mock(AlgorithmSQLContext.class), mock(EncryptContext.class)), is("늇쩗"));
    }
    
    @SphereEx(Type.MODIFY)
    @Test
    void assertEncryptWithNullPlaintext() {
        assertNull(likeEncryptAlgorithm.encrypt(null, mock(AlgorithmSQLContext.class), mock(EncryptContext.class)));
    }
    
    @SphereEx
    @Test
    void assertExpansibility() {
        String plainValue = Strings.repeat("漢", 100);
        int actualCipherCharLength = likeEncryptAlgorithm.encrypt(plainValue, mock(AlgorithmSQLContext.class), mock(EncryptContext.class)).toString().length();
        int expectedMaxCipherCharLength = likeEncryptAlgorithm.getMetaData().getExpansibility().calculate(plainValue.length(), 4);
        assertThat(actualCipherCharLength, is(100));
        assertThat(expectedMaxCipherCharLength, is(100));
        assertThat(actualCipherCharLength, is(expectedMaxCipherCharLength));
        plainValue = Strings.repeat("漢", 200);
        actualCipherCharLength = likeEncryptAlgorithm.encrypt(plainValue, mock(AlgorithmSQLContext.class), mock(EncryptContext.class)).toString().length();
        expectedMaxCipherCharLength = likeEncryptAlgorithm.getMetaData().getExpansibility().calculate(plainValue.length(), 4);
        assertThat(actualCipherCharLength, is(200));
        assertThat(expectedMaxCipherCharLength, is(200));
        assertThat(actualCipherCharLength, is(expectedMaxCipherCharLength));
    }
}
