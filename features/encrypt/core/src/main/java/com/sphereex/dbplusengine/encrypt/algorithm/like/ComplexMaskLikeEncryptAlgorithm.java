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

import com.sphereex.dbplusengine.encrypt.context.EncryptContext;
import lombok.Getter;
import org.apache.shardingsphere.encrypt.spi.EncryptAlgorithm;
import org.apache.shardingsphere.encrypt.spi.EncryptAlgorithmMetaData;
import org.apache.shardingsphere.infra.algorithm.core.config.AlgorithmConfiguration;
import org.apache.shardingsphere.infra.algorithm.core.context.AlgorithmSQLContext;
import org.apache.shardingsphere.infra.spi.type.typed.TypedSPILoader;
import org.apache.shardingsphere.mask.spi.MaskAlgorithm;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;

/**
 * Complex mask and like encrypt algorithm.
 */
public final class ComplexMaskLikeEncryptAlgorithm implements EncryptAlgorithm {
    
    private static final Collection<Character> SKIP_TRANSFORM_CHARS = new HashSet<>(3, 1F);
    
    private static final String LIKE_ALGORITHM_NAME = "like-algorithm-name";
    
    private static final String DEFAULT_MASK_ALGORITHM = "KEEP_FIRST_N_LAST_M";
    
    private static final String MASK_ALGORITHM_NAME = "mask-algorithm-name";
    
    private static final String DEFAULT_LIKE_ALGORITHM = "SphereEx:CHAR_TRANSFORM_LIKE";
    
    @Getter
    private final EncryptAlgorithmMetaData metaData = new EncryptAlgorithmMetaData(true, true, true, false, (plainCharLength, charToByteRatio) -> plainCharLength);
    
    private Properties props;
    
    @Getter
    private final Map<String, Object> udfDataModel = new HashMap<>();
    
    private MaskAlgorithm maskAlgorithm;
    
    private EncryptAlgorithm likeEncryptAlgorithm;
    
    static {
        SKIP_TRANSFORM_CHARS.add('%');
        SKIP_TRANSFORM_CHARS.add('_');
        SKIP_TRANSFORM_CHARS.add('\'');
        SKIP_TRANSFORM_CHARS.add('"');
    }
    
    @Override
    public void init(final Properties props) {
        this.props = props;
        initLikeAlgorithm(props);
        initMaskAlgorithm(props);
    }
    
    private void initLikeAlgorithm(final Properties props) {
        String likeAlgorithmName = props.getProperty(LIKE_ALGORITHM_NAME, DEFAULT_LIKE_ALGORITHM);
        likeEncryptAlgorithm = TypedSPILoader.getService(EncryptAlgorithm.class, likeAlgorithmName, createProperties(props));
    }
    
    private void initMaskAlgorithm(final Properties props) {
        String maskAlgorithmName = props.getProperty(MASK_ALGORITHM_NAME, DEFAULT_MASK_ALGORITHM);
        maskAlgorithm = TypedSPILoader.getService(MaskAlgorithm.class, maskAlgorithmName, createProperties(props));
    }
    
    private Properties createProperties(final Properties props) {
        Properties result = new Properties();
        result.putAll(props);
        return result;
    }
    
    @Override
    public String encrypt(final Object plainValue, final AlgorithmSQLContext algorithmSQLContext, final EncryptContext encryptContext) {
        return null == plainValue ? null : doEncrypt(doMask(plainValue.toString()));
    }
    
    private Object doMask(final String plainString) {
        Map<Integer, Character> ignoredChars = new TreeMap<>();
        StringBuilder commonChars = new StringBuilder(plainString.length());
        for (int i = 0; i < plainString.length(); i++) {
            if (SKIP_TRANSFORM_CHARS.contains(plainString.charAt(i))) {
                ignoredChars.put(i, plainString.charAt(i));
            } else {
                commonChars.append(plainString.charAt(i));
            }
        }
        Object masked = maskAlgorithm.mask(commonChars);
        if (null == masked) {
            return null;
        }
        StringBuilder result = new StringBuilder(masked.toString());
        if (!ignoredChars.isEmpty()) {
            ignoredChars.forEach((key, value) -> result.insert(key, value.toString()));
        }
        return result.toString();
    }
    
    private String doEncrypt(final Object mask) {
        if (null == mask) {
            return null;
        }
        Object result = likeEncryptAlgorithm.encrypt(String.valueOf(mask), null, null);
        return null == result ? null : result.toString();
    }
    
    @Override
    public Object decrypt(final Object cipherValue, final AlgorithmSQLContext algorithmSQLContext, final EncryptContext encryptContext) {
        // TODO support decrypt
        throw new UnsupportedOperationException(String.format("Algorithm `%s` is unsupported to decrypt", getType()));
    }
    
    @Override
    public AlgorithmConfiguration toConfiguration() {
        return new AlgorithmConfiguration(getType(), props);
    }
    
    @Override
    public String getType() {
        return "SphereEx:COMPLEX_MASK_LIKE";
    }
}
