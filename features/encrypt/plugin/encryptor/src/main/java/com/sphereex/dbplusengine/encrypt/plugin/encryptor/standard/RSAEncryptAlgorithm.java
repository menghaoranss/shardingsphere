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

package com.sphereex.dbplusengine.encrypt.plugin.encryptor.standard;

import com.sphereex.dbplusengine.SphereEx;
import com.sphereex.dbplusengine.encrypt.context.EncryptContext;
import lombok.Getter;
import org.apache.shardingsphere.encrypt.spi.EncryptAlgorithm;
import org.apache.shardingsphere.encrypt.spi.EncryptAlgorithmMetaData;
import org.apache.shardingsphere.infra.algorithm.core.config.AlgorithmConfiguration;
import org.apache.shardingsphere.infra.algorithm.core.context.AlgorithmSQLContext;
import org.apache.shardingsphere.infra.algorithm.cryptographic.core.CryptographicAlgorithm;
import org.apache.shardingsphere.infra.spi.type.typed.TypedSPILoader;
import org.bouncycastle.jce.provider.BouncyCastleProvider;

import java.security.Security;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * RSA encrypt algorithm.
 */
public final class RSAEncryptAlgorithm implements EncryptAlgorithm {
    
    private static final String RSA_PUBLIC_KEY = "rsa-public-key-value";
    
    private static final String RSA_PRIVATE_KEY = "rsa-private-key-value";
    
    @Getter
    private final EncryptAlgorithmMetaData metaData = new EncryptAlgorithmMetaData(true, true, false, false, this::calculateExpansibility);
    
    private Properties props;
    
    @Getter
    private Map<String, Object> udfDataModel;
    
    private CryptographicAlgorithm cryptographicAlgorithm;
    
    static {
        Security.addProvider(new BouncyCastleProvider());
    }
    
    @Override
    public void init(final Properties props) {
        this.props = props;
        udfDataModel = createUdfDataModel(props);
        cryptographicAlgorithm = TypedSPILoader.getService(CryptographicAlgorithm.class, getType(), props);
    }
    
    private Map<String, Object> createUdfDataModel(final Properties props) {
        Map<String, Object> result = new HashMap<>(2, 1F);
        result.put("rsaPublicKey", props.getProperty(RSA_PUBLIC_KEY));
        result.put("rsaPrivateKey", props.getProperty(RSA_PRIVATE_KEY));
        return result;
    }
    
    @Override
    public String encrypt(final Object plainValue, final AlgorithmSQLContext algorithmSQLContext, final EncryptContext encryptContext) {
        Object result = cryptographicAlgorithm.encrypt(plainValue);
        return null == result ? null : String.valueOf(result);
    }
    
    @Override
    public Object decrypt(final Object cipherValue, final AlgorithmSQLContext algorithmSQLContext, final EncryptContext encryptContext) {
        return cryptographicAlgorithm.decrypt(cipherValue);
    }
    
    @Override
    public AlgorithmConfiguration toConfiguration() {
        return new AlgorithmConfiguration(getType(), props);
    }
    
    @SphereEx
    private int calculateExpansibility(final int plainCharLength, final int charToByteRatio) {
        int plainByteLength = plainCharLength * charToByteRatio;
        int encryptionCount = plainByteLength % 255 == 0 ? plainByteLength / 255 : plainByteLength / 255 + 1;
        int bitSize = 2048;
        int blockSize = (bitSize + 7) / 8;
        int cipherByteLength = encryptionCount * blockSize;
        return 4 * ((cipherByteLength + 2) / 3);
    }
    
    @Override
    public String getType() {
        return "SphereEx:RSA";
    }
}
