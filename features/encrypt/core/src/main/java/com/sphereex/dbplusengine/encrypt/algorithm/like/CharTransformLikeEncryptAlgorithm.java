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

import com.google.common.base.Splitter;
import com.sphereex.dbplusengine.encrypt.context.EncryptContext;
import lombok.Getter;
import org.apache.shardingsphere.encrypt.spi.EncryptAlgorithm;
import org.apache.shardingsphere.encrypt.spi.EncryptAlgorithmMetaData;
import org.apache.shardingsphere.infra.algorithm.core.config.AlgorithmConfiguration;
import org.apache.shardingsphere.infra.algorithm.core.context.AlgorithmSQLContext;
import org.apache.shardingsphere.infra.algorithm.core.exception.AlgorithmInitializationException;
import org.apache.shardingsphere.infra.exception.core.ShardingSpherePreconditions;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;

/**
 * Char transform like encrypt algorithm.
 */
public final class CharTransformLikeEncryptAlgorithm implements EncryptAlgorithm {
    
    private static final String TRANSFORM_INDEXES = "transform-indexes";
    
    private static final String UNICODE_OFFSET = "unicode-offset";
    
    private static final String DEFAULT_TRANSFORM_INDEXES = "1,3,5,7,9,11,13,15";
    
    private static final int DEFAULT_UNICODE_OFFSET = 0x4e00;
    
    private static final int MAX_ASCII_UNICODE = 255;
    
    private static final Collection<Character> SKIP_TRANSFORM_CHARS = new HashSet<>(3, 1F);
    
    private static final int TWO_RADIX = 2;
    
    @Getter
    private final EncryptAlgorithmMetaData metaData = new EncryptAlgorithmMetaData(true, true, true, false, (plainCharLength, charToByteRatio) -> plainCharLength);
    
    private Properties props;
    
    private Collection<Integer> transformIndexes;
    
    private int unicodeOffset;
    
    @Getter
    private Map<String, Object> udfDataModel;
    
    static {
        SKIP_TRANSFORM_CHARS.add('%');
        SKIP_TRANSFORM_CHARS.add('_');
        SKIP_TRANSFORM_CHARS.add('\'');
        SKIP_TRANSFORM_CHARS.add('"');
    }
    
    @Override
    public void init(final Properties props) {
        this.props = props;
        transformIndexes = createTransformIndexes(props);
        unicodeOffset = createUnicodeOffset(props);
        udfDataModel = createUdfDataModel(props);
    }
    
    private Collection<Integer> createTransformIndexes(final Properties props) {
        Collection<Integer> result = new HashSet<>();
        for (String each : Splitter.on(",").trimResults().splitToList(props.getProperty(TRANSFORM_INDEXES, DEFAULT_TRANSFORM_INDEXES))) {
            try {
                int transformIndex = Integer.parseInt(each);
                ShardingSpherePreconditions.checkState(transformIndex >= 0 && transformIndex <= 15,
                        () -> new AlgorithmInitializationException(this, "Encrypt algorithm `CHAR_TRANSFORM_LIKE` initialization failed, reason is: transform index %s must between 0 and 7.", each));
                result.add(transformIndex);
            } catch (final NumberFormatException ex) {
                throw new AlgorithmInitializationException(this, "Encrypt algorithm `CHAR_TRANSFORM_LIKE` initialization failed, reason is: transform index %s is not a valid integer number.", each);
            }
        }
        return result;
    }
    
    private int createUnicodeOffset(final Properties props) {
        try {
            return Integer.parseInt(props.getProperty(UNICODE_OFFSET, String.valueOf(DEFAULT_UNICODE_OFFSET)));
        } catch (final NumberFormatException ex) {
            throw new AlgorithmInitializationException(
                    this, "Encrypt algorithm `CHAR_TRANSFORM_LIKE` initialization failed, reason is: unicode offset %s is not a valid integer number.", unicodeOffset);
        }
    }
    
    private Map<String, Object> createUdfDataModel(final Properties props) {
        Map<String, Object> result = new HashMap<>(2, 1F);
        result.put("transformIndexes", props.getProperty(TRANSFORM_INDEXES, DEFAULT_TRANSFORM_INDEXES));
        result.put("unicodeOffset", props.getProperty(UNICODE_OFFSET, String.valueOf(DEFAULT_UNICODE_OFFSET)));
        return result;
    }
    
    @Override
    public String encrypt(final Object plainValue, final AlgorithmSQLContext algorithmSQLContext, final EncryptContext encryptContext) {
        return null == plainValue ? null : digest(String.valueOf(plainValue));
    }
    
    @Override
    public Object decrypt(final Object cipherValue, final AlgorithmSQLContext algorithmSQLContext, final EncryptContext encryptContext) {
        // TODO support decrypt
        throw new UnsupportedOperationException(String.format("Algorithm `%s` is unsupported to decrypt", getType()));
    }
    
    private String digest(final String plainValue) {
        StringBuilder result = new StringBuilder(plainValue.length());
        for (char each : plainValue.toCharArray()) {
            char transformedChar = getTransformedChar(each);
            if (SKIP_TRANSFORM_CHARS.contains(transformedChar)) {
                result.append(each);
            } else {
                result.append(transformedChar);
            }
        }
        return result.toString();
    }
    
    private char getTransformedChar(final char originalChar) {
        // todo pipeline jdbc 这边使用缓存
        if (SKIP_TRANSFORM_CHARS.contains(originalChar)) {
            return originalChar;
        }
        String transformedBinary = transformBinary(originalChar);
        if (originalChar <= MAX_ASCII_UNICODE) {
            return (char) Integer.parseInt(transformedBinary, TWO_RADIX);
        }
        return (char) (Integer.parseInt(transformedBinary, TWO_RADIX) + unicodeOffset);
    }
    
    private String transformBinary(final char originalChar) {
        String binaryString = Integer.toBinaryString(originalChar);
        StringBuilder binaryBuilder = new StringBuilder();
        int lastIndex = 0;
        for (int index = 0; index < binaryString.length(); index++) {
            if (transformIndexes.contains(index)) {
                binaryBuilder.append(binaryString, lastIndex, index).append('0' == binaryString.charAt(index) ? '1' : '0');
                lastIndex = index + 1;
            } else if (index == binaryString.length() - 1) {
                binaryBuilder.append(binaryString.substring(lastIndex));
            }
        }
        return binaryBuilder.toString();
    }
    
    @Override
    public AlgorithmConfiguration toConfiguration() {
        return new AlgorithmConfiguration(getType(), props);
    }
    
    @Override
    public String getType() {
        return "SphereEx:CHAR_TRANSFORM_LIKE";
    }
}
