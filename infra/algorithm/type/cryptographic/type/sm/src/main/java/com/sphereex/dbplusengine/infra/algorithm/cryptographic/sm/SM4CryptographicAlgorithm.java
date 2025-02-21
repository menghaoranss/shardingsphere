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

package com.sphereex.dbplusengine.infra.algorithm.cryptographic.sm;

import com.sphereex.dbplusengine.SphereEx;
import com.sphereex.dbplusengine.SphereEx.Type;
import com.sphereex.dbplusengine.infra.algorithm.core.cache.CipherInstanceManager;
import lombok.SneakyThrows;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.apache.shardingsphere.infra.algorithm.core.config.AlgorithmConfiguration;
import org.apache.shardingsphere.infra.algorithm.core.exception.AlgorithmInitializationException;
import org.apache.shardingsphere.infra.algorithm.cryptographic.core.CryptographicAlgorithm;
import org.apache.shardingsphere.infra.exception.core.ShardingSpherePreconditions;
import org.bouncycastle.jce.provider.BouncyCastleProvider;

import javax.crypto.Cipher;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.Security;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

/**
 * SM4 encrypt algorithm.
 */
@SphereEx(Type.COPY)
public final class SM4CryptographicAlgorithm implements CryptographicAlgorithm {
    
    static {
        Security.addProvider(new BouncyCastleProvider());
    }
    
    private static final String SM4_KEY = "sm4-key";
    
    private static final String SM4_IV = "sm4-iv";
    
    private static final String SM4_MODE = "sm4-mode";
    
    private static final String SM4_PADDING = "sm4-padding";
    
    private static final int KEY_LENGTH = 16;
    
    private static final int IV_LENGTH = 16;
    
    @SphereEx
    private static final int CCM_MODE_IV_LENGTH = 8;
    
    @SphereEx(Type.MODIFY)
    private static final Set<String> MODES = new HashSet<>(Arrays.asList("ECB", "CBC", "OFB", "CFB", "GCM", "CCM"));
    
    @SphereEx(Type.MODIFY)
    private static final Set<String> PADDINGS = new HashSet<>(Arrays.asList("PKCS5Padding", "PKCS7Padding", "NoPadding"));
    
    private byte[] sm4Key;
    
    private byte[] sm4Iv;
    
    private String sm4Mode;
    
    private String sm4ModePadding;
    
    private Properties props;
    
    @Override
    public void init(final Properties props) {
        this.props = props;
        sm4Mode = createSm4Mode(props);
        String sm4Padding = createSm4Padding(props, sm4Mode);
        sm4ModePadding = "SM4/" + sm4Mode + "/" + sm4Padding;
        sm4Key = createSm4Key(props);
        sm4Iv = createSm4Iv(props, sm4Mode);
    }
    
    private String createSm4Mode(final Properties props) {
        ShardingSpherePreconditions.checkContainsKey(props, SM4_MODE, () -> new AlgorithmInitializationException(this, "%s can not be null", SM4_MODE));
        String result = String.valueOf(props.getProperty(SM4_MODE)).toUpperCase();
        ShardingSpherePreconditions.checkContains(MODES, result, () -> new AlgorithmInitializationException(this, "Mode must be either CBC or ECB"));
        return result;
    }
    
    private byte[] createSm4Key(final Properties props) {
        ShardingSpherePreconditions.checkContainsKey(props, SM4_KEY, () -> new AlgorithmInitializationException(this, "%s can not be null", SM4_KEY));
        byte[] result = fromHexString(String.valueOf(props.getProperty(SM4_KEY)));
        ShardingSpherePreconditions.checkState(KEY_LENGTH == result.length, () -> new AlgorithmInitializationException(this, "Key length must be " + KEY_LENGTH + " bytes long"));
        return result;
    }
    
    private byte[] createSm4Iv(final Properties props, final String sm4Mode) {
        // SPEX CHANGED: BEGIN
        if (!isModeWithSm4Iv(sm4Mode)) {
            // SPEX CHANGED: END
            return new byte[0];
        }
        ShardingSpherePreconditions.checkContainsKey(props, SM4_IV, () -> new AlgorithmInitializationException(this, "%s can not be null", SM4_IV));
        String sm4IvValue = String.valueOf(props.getProperty(SM4_IV));
        byte[] result = fromHexString(sm4IvValue);
        // SPEX CHANGED: BEGIN
        checkSm4IvLength(sm4Mode, result);
        // SPEX CHANGED: END
        return result;
    }
    
    @SphereEx
    private boolean isModeWithSm4Iv(final String sm4Mode) {
        return "CBC".equalsIgnoreCase(sm4Mode) || "OFB".equalsIgnoreCase(sm4Mode) || "CFB".equalsIgnoreCase(sm4Mode) || "GCM".equalsIgnoreCase(sm4Mode) || "CCM".equalsIgnoreCase(sm4Mode);
    }
    
    @SphereEx
    private void checkSm4IvLength(final String sm4Mode, final byte[] result) {
        if ("CCM".equalsIgnoreCase(sm4Mode)) {
            ShardingSpherePreconditions.checkState(CCM_MODE_IV_LENGTH == result.length,
                    () -> new AlgorithmInitializationException(this, "CCM mode Iv length must be %s bytes long", CCM_MODE_IV_LENGTH));
        } else {
            ShardingSpherePreconditions.checkState(IV_LENGTH == result.length, () -> new AlgorithmInitializationException(this, "Iv length must be " + IV_LENGTH + " bytes long"));
        }
    }
    
    private String createSm4Padding(final Properties props, final String sm4Mode) {
        ShardingSpherePreconditions.checkContainsKey(props, SM4_PADDING, () -> new AlgorithmInitializationException(this, "%s can not be null", SM4_PADDING));
        // SPEX CHANGED: BEGIN
        String result = props.getProperty(SM4_PADDING);
        ShardingSpherePreconditions.checkContains(PADDINGS, result, () -> new AlgorithmInitializationException(this, "Padding must be either PKCS5Padding or PKCS7Padding or NoPadding"));
        ShardingSpherePreconditions.checkState(!("ECB".equalsIgnoreCase(sm4Mode) && "NoPadding".equalsIgnoreCase(result)),
                () -> new AlgorithmInitializationException(this, "%s mode cannot be used with NoPadding", "ECB"));
        ShardingSpherePreconditions.checkState(!("CBC".equalsIgnoreCase(sm4Mode) && "NoPadding".equalsIgnoreCase(result)),
                () -> new AlgorithmInitializationException(this, "%s mode cannot be used with NoPadding", "CBC"));
        ShardingSpherePreconditions.checkState(!("GCM".equalsIgnoreCase(sm4Mode) && !"NoPadding".equalsIgnoreCase(result)),
                () -> new AlgorithmInitializationException(this, "%s mode must be used with NoPadding", "GCM"));
        // SPEX CHANGED: END
        return result;
    }
    
    @Override
    public String encrypt(final Object plainValue) {
        return null == plainValue ? null : Hex.encodeHexString(encrypt(String.valueOf(plainValue).getBytes(StandardCharsets.UTF_8)));
    }
    
    private byte[] encrypt(final byte[] plainValue) {
        return handle(plainValue, Cipher.ENCRYPT_MODE);
    }
    
    @Override
    public Object decrypt(final Object cipherValue) {
        return null == cipherValue ? null : new String(decrypt(fromHexString(cipherValue.toString())), StandardCharsets.UTF_8);
    }
    
    private byte[] decrypt(final byte[] cipherValue) {
        return handle(cipherValue, Cipher.DECRYPT_MODE);
    }
    
    @SneakyThrows(GeneralSecurityException.class)
    private byte[] handle(final byte[] input, final int mode) {
        boolean encryptMode = Cipher.ENCRYPT_MODE == mode;
        Cipher cipher = "GCM".equalsIgnoreCase(sm4Mode) ? createCipherInstance(encryptMode)
                : CipherInstanceManager.getInstance().getCipher(toConfiguration(), Cipher.class, () -> createCipherInstance(encryptMode), encryptMode);
        return cipher.doFinal(input);
    }
    
    @SneakyThrows(GeneralSecurityException.class)
    private Cipher getCipher(final int mode) {
        Cipher cipher = Cipher.getInstance(sm4ModePadding, BouncyCastleProvider.PROVIDER_NAME);
        SecretKeySpec secretKeySpec = new SecretKeySpec(sm4Key, "SM4");
        if (0 == sm4Iv.length) {
            cipher.init(mode, secretKeySpec);
        } else {
            cipher.init(mode, secretKeySpec, new IvParameterSpec(sm4Iv));
        }
        return cipher;
    }
    
    private byte[] fromHexString(final String s) {
        try {
            return Hex.decodeHex(s);
        } catch (final DecoderException ex) {
            throw new AlgorithmInitializationException(this, ex.getMessage());
        }
    }
    
    @SphereEx
    private Cipher createCipherInstance(final boolean encryptMode) {
        return encryptMode ? getCipher(Cipher.ENCRYPT_MODE) : getCipher(Cipher.DECRYPT_MODE);
    }
    
    @Override
    public AlgorithmConfiguration toConfiguration() {
        return new AlgorithmConfiguration(getType(), props);
    }
    
    @Override
    public String getType() {
        return "SM4";
    }
}
