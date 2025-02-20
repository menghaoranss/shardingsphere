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

package com.sphereex.dbplusengine.infra.algorithm.cryptographic.rsa;

import com.sphereex.dbplusengine.infra.algorithm.core.cache.CipherInstanceManager;
import lombok.Getter;
import lombok.SneakyThrows;
import org.apache.shardingsphere.infra.algorithm.core.config.AlgorithmConfiguration;
import org.apache.shardingsphere.infra.algorithm.core.exception.AlgorithmInitializationException;
import org.apache.shardingsphere.infra.algorithm.cryptographic.core.CryptographicAlgorithm;
import org.apache.shardingsphere.infra.exception.core.ShardingSpherePreconditions;
import org.bouncycastle.jce.provider.BouncyCastleProvider;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.security.Key;
import java.security.KeyFactory;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.Security;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.PKCS8EncodedKeySpec;
import java.security.spec.X509EncodedKeySpec;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * RSA cryptographic algorithm.
 */
public final class RSACryptographicAlgorithm implements CryptographicAlgorithm {
    
    private static final String RSA_PUBLIC_KEY = "rsa-public-key-value";
    
    private static final String RSA_PRIVATE_KEY = "rsa-private-key-value";
    
    private static final String RSA_ALGORITHM = "RSA";
    
    private PublicKey publicKey;
    
    private PrivateKey privateKey;
    
    @Getter
    private Map<String, Object> udfDataModel;
    
    private Properties props;
    
    static {
        Security.addProvider(new BouncyCastleProvider());
    }
    
    @Override
    public void init(final Properties props) {
        this.props = props;
        publicKey = generatePublicKey(props);
        privateKey = generatePrivateKey(props);
        udfDataModel = createUdfDataModel(props);
    }
    
    private PublicKey generatePublicKey(final Properties props) {
        ShardingSpherePreconditions.checkNotNull(props.getProperty(RSA_PUBLIC_KEY), () -> new AlgorithmInitializationException(this, "%s can not be null", RSA_PUBLIC_KEY));
        byte[] key = base64Decode(props.getProperty(RSA_PUBLIC_KEY), true);
        try {
            return KeyFactory.getInstance(RSA_ALGORITHM, BouncyCastleProvider.PROVIDER_NAME).generatePublic(new X509EncodedKeySpec(key));
        } catch (final InvalidKeySpecException | NoSuchAlgorithmException | NoSuchProviderException ex) {
            throw new AlgorithmInitializationException(this, "can not generate public key, " + ex.getMessage());
        }
    }
    
    private PrivateKey generatePrivateKey(final Properties props) {
        ShardingSpherePreconditions.checkNotNull(props.getProperty(RSA_PRIVATE_KEY), () -> new AlgorithmInitializationException(this, "%s can not be null", RSA_PRIVATE_KEY));
        byte[] key = base64Decode(props.getProperty(RSA_PRIVATE_KEY), false);
        try {
            return KeyFactory.getInstance(RSA_ALGORITHM, BouncyCastleProvider.PROVIDER_NAME).generatePrivate(new PKCS8EncodedKeySpec(key));
        } catch (final InvalidKeySpecException | NoSuchAlgorithmException | NoSuchProviderException ex) {
            throw new AlgorithmInitializationException(this, "can not generate private key, " + ex.getMessage());
        }
    }
    
    private byte[] base64Decode(final String key, final boolean isPublicKey) {
        try {
            return Base64.getDecoder().decode(key);
            // CHECKSTYLE:OFF
        } catch (Exception ex) {
            // CHECKSTYLE:ON
            throw new AlgorithmInitializationException(this, String.format("%s key is not in Base64 encoding format, %s", isPublicKey ? "public" : "private", ex.getMessage()));
        }
    }
    
    private Map<String, Object> createUdfDataModel(final Properties props) {
        Map<String, Object> result = new HashMap<>(2, 1F);
        result.put("rsaPublicKey", props.getProperty(RSA_PUBLIC_KEY));
        result.put("rsaPrivateKey", props.getProperty(RSA_PRIVATE_KEY));
        return result;
    }
    
    @SneakyThrows({GeneralSecurityException.class, IOException.class})
    @Override
    public String encrypt(final Object plainValue) {
        if (null == plainValue) {
            return null;
        }
        Cipher cipher = CipherInstanceManager.getInstance().getEncryptCipher(toConfiguration(), Cipher.class, () -> createCipherInstance(true));
        return Base64.getEncoder().encodeToString(doFinal(plainValue.toString().getBytes(), cipher));
    }
    
    private byte[] doFinal(final byte[] data, final Cipher cipher) throws IllegalBlockSizeException, BadPaddingException, IOException {
        int maxBlockSize = cipher.getBlockSize();
        if (data.length <= maxBlockSize) {
            return cipher.doFinal(data, 0, data.length);
        }
        return doFinalWithBlock(data, maxBlockSize, cipher);
    }
    
    private byte[] doFinalWithBlock(final byte[] data, final int maxBlockSize, final Cipher cipher) throws IllegalBlockSizeException, BadPaddingException, IOException {
        int dataLength = data.length;
        try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            int offSet = 0;
            int remainingLength = dataLength;
            int blockSize;
            while (remainingLength > 0) {
                blockSize = Math.min(remainingLength, maxBlockSize);
                out.write(cipher.doFinal(data, offSet, blockSize));
                offSet += blockSize;
                remainingLength = dataLength - offSet;
            }
            return out.toByteArray();
        }
    }
    
    @SneakyThrows({GeneralSecurityException.class, IOException.class})
    @Override
    public Object decrypt(final Object cipherValue) {
        if (null == cipherValue) {
            return null;
        }
        Cipher cipher = CipherInstanceManager.getInstance().getDecryptCipher(toConfiguration(), Cipher.class, () -> createCipherInstance(false));
        return new String(doFinal(Base64.getDecoder().decode(cipherValue.toString()), cipher));
    }
    
    /**
     * Create cipher instance.
     *
     * @param encryptMode encrypt mode
     * @return cipher instance
     */
    @SneakyThrows(GeneralSecurityException.class)
    public Cipher createCipherInstance(final boolean encryptMode) {
        return encryptMode ? getCipher(Cipher.ENCRYPT_MODE, publicKey) : getCipher(Cipher.DECRYPT_MODE, privateKey);
    }
    
    private Cipher getCipher(final int encryptMode, final Key key) throws GeneralSecurityException {
        Cipher cipher = Cipher.getInstance(RSA_ALGORITHM, BouncyCastleProvider.PROVIDER_NAME);
        cipher.init(encryptMode, key);
        return cipher;
    }
    
    @Override
    public AlgorithmConfiguration toConfiguration() {
        return new AlgorithmConfiguration(getType(), props);
    }
    
    @Override
    public String getType() {
        return "SphereEx:RSA";
    }
}
