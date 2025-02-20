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

package org.apache.shardingsphere.encrypt.algorithm.standard;

import com.sphereex.dbplusengine.SphereEx;
import com.sphereex.dbplusengine.SphereEx.Type;
import com.sphereex.dbplusengine.encrypt.context.EncryptContext;
import lombok.Getter;
import org.apache.shardingsphere.encrypt.spi.EncryptAlgorithm;
import org.apache.shardingsphere.encrypt.spi.EncryptAlgorithmMetaData;
import org.apache.shardingsphere.infra.algorithm.core.config.AlgorithmConfiguration;
import org.apache.shardingsphere.infra.algorithm.core.context.AlgorithmSQLContext;
import org.apache.shardingsphere.infra.algorithm.cryptographic.core.CryptographicAlgorithm;
import org.apache.shardingsphere.infra.spi.type.typed.TypedSPILoader;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * AES encrypt algorithm.
 */
public final class AESEncryptAlgorithm implements EncryptAlgorithm {
    
    @SphereEx
    private static final String AES_KEY = "aes-key-value";
    
    @SphereEx
    private static final String DIGEST_ALGORITHM_NAME = "digest-algorithm-name";
    
    @SphereEx
    private static final String AES_KEY_BIT_LENGTH = "aes-key-bit-length";
    
    @SphereEx
    private static final int DEFAULT_AES_KEY_BIT_LENGTH = 128;
    
    @SphereEx(Type.MODIFY)
    @Getter
    private final EncryptAlgorithmMetaData metaData =
            new EncryptAlgorithmMetaData(true, true, false, false, this::calculateExpansibility);
    
    private Properties props;
    
    private CryptographicAlgorithm cryptographicAlgorithm;
    
    @SphereEx
    @Getter
    private Map<String, Object> udfDataModel;
    
    @Override
    public void init(final Properties props) {
        this.props = props;
        cryptographicAlgorithm = TypedSPILoader.getService(CryptographicAlgorithm.class, getType(), props);
        // SPEX ADDED: BEGIN
        udfDataModel = createUdfDataModel(props);
        // SPEX ADDED: END
    }
    
    @SphereEx
    private int calculateExpansibility(final int plainCharLength, final Integer charToByteRatio) {
        int plainByteLength = plainCharLength * charToByteRatio;
        int cipherByteLength = plainByteLength + 16 - plainByteLength % 16 + 2;
        return 4 * (cipherByteLength / 3);
    }
    
    @SphereEx
    private Map<String, Object> createUdfDataModel(final Properties props) {
        Map<String, Object> result = new HashMap<>(3, 1F);
        result.put("aesKey", props.getProperty(AES_KEY));
        result.put("aesKeyBitLength", props.getProperty(AES_KEY_BIT_LENGTH, String.valueOf(DEFAULT_AES_KEY_BIT_LENGTH)));
        result.put("digestAlgorithm", props.getProperty(DIGEST_ALGORITHM_NAME));
        return result;
    }
    
    @Override
    public String encrypt(final Object plainValue, final AlgorithmSQLContext algorithmSQLContext, @SphereEx final EncryptContext encryptContext) {
        Object result = cryptographicAlgorithm.encrypt(plainValue);
        return null == result ? null : String.valueOf(result);
    }
    
    @Override
    public Object decrypt(final Object cipherValue, final AlgorithmSQLContext algorithmSQLContext, @SphereEx final EncryptContext encryptContext) {
        return cryptographicAlgorithm.decrypt(cipherValue);
    }
    
    @Override
    public AlgorithmConfiguration toConfiguration() {
        return new AlgorithmConfiguration(getType(), props);
    }
    
    @Override
    public String getType() {
        return "AES";
    }
}
