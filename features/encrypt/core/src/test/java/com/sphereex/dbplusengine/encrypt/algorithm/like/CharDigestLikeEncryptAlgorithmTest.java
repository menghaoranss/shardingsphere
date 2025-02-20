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
import org.apache.shardingsphere.test.util.PropertiesBuilder;
import org.apache.shardingsphere.test.util.PropertiesBuilder.Property;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;

@SphereEx(Type.COPY)
class CharDigestLikeEncryptAlgorithmTest {
    
    private EncryptAlgorithm englishLikeEncryptAlgorithm;
    
    private EncryptAlgorithm chineseLikeEncryptAlgorithm;
    
    private EncryptAlgorithm koreanLikeEncryptAlgorithm;
    
    @BeforeEach
    void setUp() {
        englishLikeEncryptAlgorithm = TypedSPILoader.getService(EncryptAlgorithm.class, "CHAR_DIGEST_LIKE");
        chineseLikeEncryptAlgorithm = TypedSPILoader.getService(EncryptAlgorithm.class, "CHAR_DIGEST_LIKE");
        koreanLikeEncryptAlgorithm = TypedSPILoader.getService(EncryptAlgorithm.class,
                "CHAR_DIGEST_LIKE", PropertiesBuilder.build(new Property("dict", "한국어시험"), new Property("start", "44032")));
    }
    
    @SphereEx(Type.MODIFY)
    @Test
    void assertEncrypt() {
        assertThat(englishLikeEncryptAlgorithm.encrypt("1234567890%abcdefghijklmnopqrstuvwxyz%ABCDEFGHIJKLMNOPQRSTUVWXYZ",
                mock(AlgorithmSQLContext.class), mock(EncryptContext.class)), is("0145458981%`adedehihilmlmpqpqtutuxyxy%@ADEDEHIHILMLMPQPQTUTUXYXY"));
        assertThat(englishLikeEncryptAlgorithm.encrypt("_1234__5678__", mock(AlgorithmSQLContext.class), mock(EncryptContext.class)), is("_0145__4589__"));
    }
    
    @SphereEx(Type.MODIFY)
    @Test
    void assertEncryptWithChineseChar() {
        assertThat(chineseLikeEncryptAlgorithm.encrypt("中国", mock(AlgorithmSQLContext.class), mock(EncryptContext.class)), is("婝估"));
    }
    
    @SphereEx(Type.MODIFY)
    @Test
    void assertEncryptWithKoreanChar() {
        assertThat(koreanLikeEncryptAlgorithm.encrypt("한국", mock(AlgorithmSQLContext.class), mock(EncryptContext.class)), is("각가"));
    }
    
    @SphereEx(Type.MODIFY)
    @Test
    void assertEncryptWithNullPlaintext() {
        assertNull(englishLikeEncryptAlgorithm.encrypt(null, mock(AlgorithmSQLContext.class), mock(EncryptContext.class)));
    }
    
    @SphereEx
    @Test
    void assertExpansibility() {
        String plainValue = Strings.repeat("漢", 100);
        int actualCipherCharLength = chineseLikeEncryptAlgorithm.encrypt(plainValue, mock(AlgorithmSQLContext.class), mock(EncryptContext.class)).toString().length();
        int expectedMaxCipherCharLength = chineseLikeEncryptAlgorithm.getMetaData().getExpansibility().calculate(plainValue.length(), 4);
        assertThat(actualCipherCharLength, is(expectedMaxCipherCharLength));
        plainValue = Strings.repeat("漢", 200);
        actualCipherCharLength = chineseLikeEncryptAlgorithm.encrypt(plainValue, mock(AlgorithmSQLContext.class), mock(EncryptContext.class)).toString().length();
        expectedMaxCipherCharLength = chineseLikeEncryptAlgorithm.getMetaData().getExpansibility().calculate(plainValue.length(), 4);
        assertThat(actualCipherCharLength, is(expectedMaxCipherCharLength));
    }
}
