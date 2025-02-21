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

package com.sphereex.dbplusengine.infra.algorithm.cryptographic.aes.props;

import org.apache.shardingsphere.infra.algorithm.core.exception.AlgorithmInitializationException;
import org.apache.shardingsphere.infra.algorithm.cryptographic.core.CryptographicPropertiesProvider;
import org.apache.shardingsphere.infra.exception.core.ShardingSpherePreconditions;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Properties;

/**
 * Abstract MySQL trunk type AES properties provider.
 */
public abstract class AbstractMySQLTrunkTypeAESPropertiesProvider implements CryptographicPropertiesProvider {
    
    private static final String AES_KEY = "aes-key-value";
    
    private static final String AES_MODE = "aes-mode";
    
    private static final String AES_IV = "aes-iv";
    
    private static final int IV_SIZE = 16;
    
    private static final String AES_KEY_BIT_LENGTH = "aes-key-bit-length";
    
    private static final int DEFAULT_AES_KEY_BIT_LENGTH = 128;
    
    private static final Collection<Integer> CANDIDATE_AES_KEY_BIT_LENGTH = new HashSet<>(Arrays.asList(128, 192, 256));
    
    private static final Collection<String> SUPPORTED_MODES = Arrays.asList("ECB", "CBC", "CTR");
    
    private static final String AES_ENCODER = "aes-encoder";
    
    private static final Collection<String> SUPPORTED_ENCODERS = Arrays.asList("BASE64", "HEX");
    
    protected byte[] getSecretKey(final Properties props, final int secretKeyByteLength) {
        String aesKey = props.getProperty(AES_KEY);
        ShardingSpherePreconditions.checkNotEmpty(aesKey, () -> new AlgorithmInitializationException(this, "%s can not be null or empty", AES_KEY));
        return generateMariaDBSecretKey(aesKey, secretKeyByteLength);
    }
    
    protected byte[] generateMariaDBSecretKey(final String key, final int secretKeyByteLength) {
        byte[] result = new byte[secretKeyByteLength];
        int i = 0;
        for (byte each : key.getBytes(StandardCharsets.UTF_8)) {
            result[i++ % secretKeyByteLength] ^= each;
        }
        return result;
    }
    
    protected byte[] getIvParameter(final Properties props) {
        if (props.containsKey(AES_IV)) {
            String ivParameter = props.getProperty(AES_IV);
            ShardingSpherePreconditions.checkState(ivParameter.length() >= IV_SIZE, () -> new AlgorithmInitializationException(this, "the length of %s must be greater than or equal to 16", AES_IV));
            return ivParameter.substring(0, IV_SIZE).getBytes();
        }
        return new byte[0];
    }
    
    protected String getMode(final Properties props) {
        String result = props.getProperty(AES_MODE, "ECB");
        ShardingSpherePreconditions.checkContains(getSupportedModes(), result, () -> new AlgorithmInitializationException(this, String.format("Mode must be `%s`", SUPPORTED_MODES), AES_MODE));
        return result;
    }
    
    protected String getPadding(final String mode) {
        return "ECB".equals(mode) ? "" : "PKCS7Padding";
    }
    
    protected int getKeyBitLength(final Properties props) {
        int result = Integer.parseInt(props.getProperty(AES_KEY_BIT_LENGTH, String.valueOf(DEFAULT_AES_KEY_BIT_LENGTH)));
        ShardingSpherePreconditions.checkContains(CANDIDATE_AES_KEY_BIT_LENGTH, result,
                () -> new AlgorithmInitializationException(this, "%s should in %s", AES_KEY_BIT_LENGTH, CANDIDATE_AES_KEY_BIT_LENGTH));
        return result;
    }
    
    protected String getEncoder(final Properties props) {
        String result = props.getProperty(AES_ENCODER, "BASE64");
        ShardingSpherePreconditions.checkContains(SUPPORTED_ENCODERS, result, () -> new AlgorithmInitializationException(this, String.format("Encoder must be `%s`", SUPPORTED_ENCODERS), AES_ENCODER));
        return result;
    }
    
    /**
     * Get supported modes.
     *
     * @return supported modes.
     */
    protected abstract Collection<String> getSupportedModes();
}
