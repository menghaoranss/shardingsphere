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

import lombok.Getter;
import org.apache.shardingsphere.infra.algorithm.core.exception.AlgorithmInitializationException;
import org.apache.shardingsphere.infra.algorithm.cryptographic.core.CryptographicPropertiesProvider;
import org.apache.shardingsphere.infra.exception.core.ShardingSpherePreconditions;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collection;
import java.util.Properties;

/**
 * OceanBase Oracle mode AES properties provider.
 */
public final class OceanBaseOracleModeAESPropertiesProvider implements CryptographicPropertiesProvider {
    
    private static final String AES_KEY = "aes-key-value";
    
    private static final int OCEAN_BASE_AES_KEY_BIT_LENGTH = 256;
    
    private static final String OCEAN_BASE_AES_IV = "000102030405060708090A0B0C0D0E0F";
    
    private static final String AES_ENCODER = "aes-encoder";
    
    private static final Collection<String> SUPPORTED_ENCODERS = Arrays.asList("BASE64", "HEX");
    
    @Getter
    private byte[] secretKey;
    
    @Getter
    private String mode;
    
    @Getter
    private String padding;
    
    @Getter
    private byte[] ivParameter;
    
    @Getter
    private String encoder;
    
    private int secretKeyByteLength;
    
    @Override
    public void init(final Properties props) {
        secretKeyByteLength = OCEAN_BASE_AES_KEY_BIT_LENGTH / 8;
        secretKey = getSecretKey(props);
        mode = "CBC";
        ivParameter = generateIvParameter();
        padding = "PKCS5Padding";
        encoder = getEncoder(props);
    }
    
    private byte[] getSecretKey(final Properties props) {
        String aesKey = props.getProperty(AES_KEY);
        ShardingSpherePreconditions.checkNotEmpty(aesKey, () -> new AlgorithmInitializationException(this, "%s can not be null or empty", AES_KEY));
        return generateOceanBaseSecretKey(aesKey);
    }
    
    private byte[] generateOceanBaseSecretKey(final String key) {
        int length = secretKeyByteLength;
        byte[] result = key.getBytes(StandardCharsets.UTF_8);
        if (result.length < length) {
            result = Arrays.copyOf(result, length);
            Arrays.fill(result, result.length, length, (byte) 0);
            return result;
        }
        if (result.length > length) {
            return Arrays.copyOf(result, length);
        }
        return result;
    }
    
    private byte[] generateIvParameter() {
        return hexToRaw(OCEAN_BASE_AES_IV);
    }
    
    private byte[] hexToRaw(final String hexString) {
        if (0 != hexString.length() % 2) {
            throw new IllegalArgumentException("Hex string must have an even length.");
        }
        byte[] result = new byte[hexString.length() / 2];
        for (int i = 0; i < hexString.length(); i += 2) {
            int byteValue = Integer.parseInt(hexString.substring(i, i + 2), 16);
            result[i / 2] = (byte) byteValue;
        }
        return result;
    }
    
    private String getEncoder(final Properties props) {
        String result = props.getProperty(AES_ENCODER, "HEX");
        ShardingSpherePreconditions.checkContains(SUPPORTED_ENCODERS, result, () -> new AlgorithmInitializationException(this, String.format("Encoder must be `%s`", SUPPORTED_ENCODERS), AES_ENCODER));
        return result;
    }
    
    @Override
    public String getType() {
        return "OceanBase_Oracle";
    }
}
