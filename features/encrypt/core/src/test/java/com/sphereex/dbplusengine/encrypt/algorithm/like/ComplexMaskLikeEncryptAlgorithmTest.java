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
import com.sphereex.dbplusengine.encrypt.context.EncryptContext;
import org.apache.shardingsphere.encrypt.spi.EncryptAlgorithm;
import org.apache.shardingsphere.infra.algorithm.core.context.AlgorithmSQLContext;
import org.apache.shardingsphere.infra.spi.type.typed.TypedSPILoader;
import org.apache.shardingsphere.test.util.PropertiesBuilder;
import org.junit.jupiter.api.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;

class ComplexMaskLikeEncryptAlgorithmTest {
    
    @Test
    void assertEncryptChineseName() {
        EncryptAlgorithm likeEncryptAlgorithm = TypedSPILoader.getService(EncryptAlgorithm.class, "SphereEx:COMPLEX_MASK_LIKE",
                PropertiesBuilder.build(new PropertiesBuilder.Property("mask-algorithm-name", "SphereEx:MASK_CHINESE_NAME")));
        assertThat(likeEncryptAlgorithm.encrypt("张三", mock(AlgorithmSQLContext.class), mock(EncryptContext.class)),
                is("쎊?"));
        assertThat(likeEncryptAlgorithm.encrypt("张三丰", mock(AlgorithmSQLContext.class), mock(EncryptContext.class)), is("쎊?늚"));
        assertThat(likeEncryptAlgorithm.encrypt("张%", mock(AlgorithmSQLContext.class), mock(EncryptContext.class)), is("쎊%"));
        assertThat(likeEncryptAlgorithm.encrypt("%张", mock(AlgorithmSQLContext.class), mock(EncryptContext.class)), is("%쎊"));
        assertThat(likeEncryptAlgorithm.encrypt("%张%", mock(AlgorithmSQLContext.class), mock(EncryptContext.class)), is("%쎊%"));
        assertThat(likeEncryptAlgorithm.encrypt("_张", mock(AlgorithmSQLContext.class), mock(EncryptContext.class)), is("_쎊"));
        assertThat(likeEncryptAlgorithm.encrypt("_张_", mock(AlgorithmSQLContext.class), mock(EncryptContext.class)), is("_쎊_"));
        assertThat(likeEncryptAlgorithm.encrypt("%张三丰%", mock(AlgorithmSQLContext.class), mock(EncryptContext.class)), is("%쎊?늚%"));
    }
    
    @Test
    void assertEncryptWithDefaultMaskAlgorithm() {
        EncryptAlgorithm likeEncryptAlgorithm = TypedSPILoader.getService(EncryptAlgorithm.class, "SphereEx:COMPLEX_MASK_LIKE",
                PropertiesBuilder.build(new PropertiesBuilder.Property("first-n", "3"), new PropertiesBuilder.Property("last-m", "4"), new PropertiesBuilder.Property("replace-char", "*")));
        assertThat(likeEncryptAlgorithm.encrypt("321302199909091234", mock(AlgorithmSQLContext.class), mock(EncryptContext.class)), is("&2$???????????$2&!"));
        assertThat(likeEncryptAlgorithm.encrypt("%3213%", mock(AlgorithmSQLContext.class), mock(EncryptContext.class)), is("%&2$&%"));
        assertThat(likeEncryptAlgorithm.encrypt("%2130%", mock(AlgorithmSQLContext.class), mock(EncryptContext.class)), is("%2$&0%"));
        assertThat(likeEncryptAlgorithm.encrypt("%1234%", mock(AlgorithmSQLContext.class), mock(EncryptContext.class)), is("%$2&!%"));
        assertThat(likeEncryptAlgorithm.encrypt("%1234567890%", mock(AlgorithmSQLContext.class), mock(EncryptContext.class)), is("%$2&???7-,0%"));
    }
    
    @SphereEx
    @Test
    void assertExpansibility() {
        EncryptAlgorithm likeEncryptAlgorithm = TypedSPILoader.getService(EncryptAlgorithm.class, "SphereEx:COMPLEX_MASK_LIKE",
                PropertiesBuilder.build(new PropertiesBuilder.Property("first-n", "3"), new PropertiesBuilder.Property("last-m", "4"), new PropertiesBuilder.Property("replace-char", "*")));
        String plainValue = Strings.repeat("漢", 100);
        assertThat(likeEncryptAlgorithm.encrypt(plainValue, mock(AlgorithmSQLContext.class), mock(EncryptContext.class)).toString().length(),
                is(likeEncryptAlgorithm.getMetaData().getExpansibility().calculate(plainValue.length(), 0)));
    }
}
