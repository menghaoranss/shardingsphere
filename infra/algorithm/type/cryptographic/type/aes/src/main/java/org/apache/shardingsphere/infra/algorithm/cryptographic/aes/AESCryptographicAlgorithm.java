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

package org.apache.shardingsphere.infra.algorithm.cryptographic.aes;

import com.google.common.base.Strings;
import com.sphereex.dbplusengine.SphereEx;
import com.sphereex.dbplusengine.infra.algorithm.core.cache.CipherInstanceManager;
import lombok.SneakyThrows;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.apache.shardingsphere.infra.algorithm.core.config.AlgorithmConfiguration;
import org.apache.shardingsphere.infra.algorithm.cryptographic.core.CryptographicAlgorithm;
import org.apache.shardingsphere.infra.algorithm.cryptographic.core.CryptographicPropertiesProvider;
import org.apache.shardingsphere.infra.spi.type.typed.TypedSPILoader;
import org.bouncycastle.jce.provider.BouncyCastleProvider;

import javax.crypto.Cipher;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.Security;
import java.util.Base64;
import java.util.Properties;

/**
 * AES cryptographic algorithm.
 */
public final class AESCryptographicAlgorithm implements CryptographicAlgorithm {
    
    @SphereEx
    private Properties props;
    
    private CryptographicPropertiesProvider propsProvider;
    
    // SPEX ADDED: BEGIN
    static {
        Security.addProvider(new BouncyCastleProvider());
    }
    // SPEX ADDED: END
    
    @Override
    public void init(final Properties props) {
        // SPEX ADDED: BEGIN
        this.props = props;
        String compatibleMode = props.containsKey("db-compatible-mode") ? props.getProperty("db-compatible-mode") : "DEFAULT";
        // SPEX ADDED: END
        // SPEX CHANGED: BEGIN
        propsProvider = TypedSPILoader.getService(CryptographicPropertiesProvider.class, compatibleMode, props);
        // SPEX CHANGED: END
    }
    
    @SneakyThrows(GeneralSecurityException.class)
    @Override
    public String encrypt(final Object plainValue) {
        if (null == plainValue) {
            return null;
        }
        // SPEX CHANGED: BEGIN
        byte[] result = CipherInstanceManager.getInstance().getEncryptCipher(toConfiguration(), Cipher.class, () -> createCipherInstance(true))
                .doFinal(String.valueOf(plainValue).getBytes(StandardCharsets.UTF_8));
        // SPEX CHANGED: END
        return encode(result);
    }
    
    private String encode(final byte[] value) {
        // SPEX ADDED: BEGIN
        if ("HEX".equals(propsProvider.getEncoder())) {
            return Hex.encodeHexString(value, false);
        }
        // SPEX ADDED: END
        return Base64.getEncoder().encodeToString(value);
    }
    
    @SneakyThrows(GeneralSecurityException.class)
    @Override
    public Object decrypt(final Object cipherValue) {
        if (null == cipherValue) {
            return null;
        }
        // SPEX CHANGED: BEGIN
        byte[] result = CipherInstanceManager.getInstance().getDecryptCipher(toConfiguration(), Cipher.class, () -> createCipherInstance(false))
                .doFinal(decode(cipherValue.toString().trim()));
        // SPEX CHANGED: END
        return new String(result, StandardCharsets.UTF_8);
    }
    
    // SPEX ADDED: BEGIN
    @SneakyThrows(DecoderException.class)
    // SPEX ADDED: END
    private byte[] decode(final String value) {
        // SPEX ADDED: BEGIN
        if ("HEX".equals(propsProvider.getEncoder())) {
            return Hex.decodeHex(value);
        }
        // SPEX ADDED: END
        return Base64.getDecoder().decode(value);
    }
    
    @SphereEx
    @Override
    public AlgorithmConfiguration toConfiguration() {
        return new AlgorithmConfiguration(getType(), props);
    }
    
    @SphereEx
    @SneakyThrows(GeneralSecurityException.class)
    private Cipher createCipherInstance(final boolean encryptMode) {
        return encryptMode ? getCipher(Cipher.ENCRYPT_MODE) : getCipher(Cipher.DECRYPT_MODE);
    }
    
    private Cipher getCipher(final int decryptMode) throws GeneralSecurityException {
        // SPEX ADDED: BEGIN
        if (!Strings.isNullOrEmpty(propsProvider.getMode()) && !Strings.isNullOrEmpty(propsProvider.getPadding())) {
            return getCipherWithModePadding(decryptMode);
        }
        // SPEX ADDED: END
        Cipher result = Cipher.getInstance(getType());
        result.init(decryptMode, new SecretKeySpec(propsProvider.getSecretKey(), getType()));
        return result;
    }
    
    @SphereEx
    private Cipher getCipherWithModePadding(final int decryptMode) throws GeneralSecurityException {
        String modePadding = "AES/" + propsProvider.getMode() + "/" + propsProvider.getPadding();
        Cipher result = Cipher.getInstance(modePadding);
        if (0 == propsProvider.getIvParameter().length) {
            result.init(decryptMode, new SecretKeySpec(propsProvider.getSecretKey(), getType()));
        } else {
            result.init(decryptMode, new SecretKeySpec(propsProvider.getSecretKey(), getType()), new IvParameterSpec(propsProvider.getIvParameter()));
        }
        return result;
    }
    
    @Override
    public String getType() {
        return "AES";
    }
}
