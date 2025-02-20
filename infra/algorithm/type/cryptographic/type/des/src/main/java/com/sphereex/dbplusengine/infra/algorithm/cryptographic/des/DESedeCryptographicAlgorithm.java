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

package com.sphereex.dbplusengine.infra.algorithm.cryptographic.des;

import com.sphereex.dbplusengine.infra.algorithm.core.cache.CipherInstanceManager;
import lombok.SneakyThrows;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.codec.digest.MessageDigestAlgorithms;
import org.apache.shardingsphere.infra.algorithm.core.config.AlgorithmConfiguration;
import org.apache.shardingsphere.infra.algorithm.core.exception.AlgorithmInitializationException;
import org.apache.shardingsphere.infra.algorithm.cryptographic.core.CryptographicAlgorithm;
import org.apache.shardingsphere.infra.exception.core.ShardingSpherePreconditions;
import org.bouncycastle.jce.provider.BouncyCastleProvider;

import javax.crypto.Cipher;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.Security;
import java.util.Arrays;
import java.util.Base64;
import java.util.Properties;

/**
 * DESede cryptographic algorithm.
 */
public final class DESedeCryptographicAlgorithm implements CryptographicAlgorithm {
    
    private static final String DESEDE_KEY = "desede-key-value";
    
    private static final String DIGEST_ALGORITHM_NAME = "digest-algorithm-name";
    
    private static final String ALGORITHM_NAME = "DESEDE";
    
    private static final int DEFAULT_DESEDE_KEY_BIT_LENGTH = 192;
    
    private byte[] secretKey;
    
    private Properties props;
    
    static {
        Security.addProvider(new BouncyCastleProvider());
    }
    
    @Override
    public void init(final Properties props) {
        this.props = props;
        String desKey = props.getProperty(DESEDE_KEY);
        ShardingSpherePreconditions.checkNotEmpty(desKey, () -> new AlgorithmInitializationException(this, String.format("%s can not be null or empty", DESEDE_KEY)));
        String digestAlgorithm = props.getProperty(DIGEST_ALGORITHM_NAME, MessageDigestAlgorithms.SHA_256);
        secretKey = Arrays.copyOf(DigestUtils.getDigest(digestAlgorithm.toUpperCase()).digest(desKey.getBytes(StandardCharsets.UTF_8)), DEFAULT_DESEDE_KEY_BIT_LENGTH / 8);
    }
    
    @SneakyThrows(GeneralSecurityException.class)
    @Override
    public String encrypt(final Object plainValue) {
        if (null == plainValue) {
            return null;
        }
        byte[] result = CipherInstanceManager.getInstance().getEncryptCipher(toConfiguration(), Cipher.class, () -> createCipherInstance(true))
                .doFinal(String.valueOf(plainValue).getBytes(StandardCharsets.UTF_8));
        return Base64.getEncoder().encodeToString(result);
    }
    
    @SneakyThrows(GeneralSecurityException.class)
    @Override
    public Object decrypt(final Object cipherValue) {
        if (null == cipherValue) {
            return null;
        }
        byte[] result = CipherInstanceManager.getInstance().getDecryptCipher(toConfiguration(), Cipher.class, () -> createCipherInstance(false))
                .doFinal(Base64.getDecoder().decode(cipherValue.toString().trim()));
        return new String(result, StandardCharsets.UTF_8);
    }
    
    @SneakyThrows(GeneralSecurityException.class)
    private Cipher createCipherInstance(final boolean encryptMode) {
        return encryptMode ? getCipher(Cipher.ENCRYPT_MODE) : getCipher(Cipher.DECRYPT_MODE);
    }
    
    private Cipher getCipher(final int encryptMode) throws GeneralSecurityException {
        Cipher result = Cipher.getInstance(ALGORITHM_NAME, BouncyCastleProvider.PROVIDER_NAME);
        result.init(encryptMode, new SecretKeySpec(secretKey, ALGORITHM_NAME));
        return result;
    }
    
    @Override
    public AlgorithmConfiguration toConfiguration() {
        return new AlgorithmConfiguration(getType(), props);
    }
    
    @Override
    public String getType() {
        return "SphereEx:DESEDE";
    }
}
