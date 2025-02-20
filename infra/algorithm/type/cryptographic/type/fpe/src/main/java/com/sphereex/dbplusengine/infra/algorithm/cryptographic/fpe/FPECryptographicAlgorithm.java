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

package com.sphereex.dbplusengine.infra.algorithm.cryptographic.fpe;

import com.sphereex.dbplusengine.infra.algorithm.core.cache.CipherInstanceManager;
import lombok.Getter;
import lombok.SneakyThrows;
import org.apache.shardingsphere.infra.algorithm.core.config.AlgorithmConfiguration;
import org.apache.shardingsphere.infra.algorithm.core.exception.AlgorithmInitializationException;
import org.apache.shardingsphere.infra.algorithm.cryptographic.core.CryptographicAlgorithm;
import org.apache.shardingsphere.infra.exception.core.ShardingSpherePreconditions;
import org.bouncycastle.crypto.AlphabetMapper;
import org.bouncycastle.crypto.util.BasicAlphabetMapper;
import org.bouncycastle.jcajce.spec.FPEParameterSpec;
import org.bouncycastle.jce.provider.BouncyCastleProvider;

import javax.crypto.Cipher;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.Security;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;

/**
 * FPE cryptographic algorithm.
 */
public final class FPECryptographicAlgorithm implements CryptographicAlgorithm {
    
    private static final String FPE_MODE = "fpe-mode";
    
    private static final String FPE_KEY = "fpe-key-value";
    
    private static final String FPE_ALPHABET = "fpe-alphabet";
    
    private static final String FPE_TWEAK = "fpe-tweak";
    
    private static final String FPE_CIPHER = "fpe-cipher";
    
    private static final String FF1_MODE = "FF1";
    
    private static final String FF3_1_MODE = "FF3-1";
    
    private static final int BYTE_BIT_SIZE = 8;
    
    private static final Collection<String> VALID_FPE_MODES = new HashSet<>(2, 1F);
    
    private static final Collection<Integer> VALID_FPE_KEY_LENGTHS = new HashSet<>(3, 1F);
    
    private static final Collection<String> VALID_FPE_CIPHERS = new HashSet<>(2, 1F);
    
    static {
        Security.addProvider(new BouncyCastleProvider());
        VALID_FPE_MODES.add(FF1_MODE);
        VALID_FPE_MODES.add(FF3_1_MODE);
        VALID_FPE_KEY_LENGTHS.add(128);
        VALID_FPE_KEY_LENGTHS.add(192);
        VALID_FPE_KEY_LENGTHS.add(256);
        VALID_FPE_CIPHERS.add("AES");
        VALID_FPE_CIPHERS.add("SM4");
    }
    
    private String fpeMode;
    
    private byte[] fpeKey;
    
    private AlphabetMapper alphabetMapper;
    
    private byte[] fpeTweak;
    
    private String fpeCipher;
    
    @Getter
    private Map<String, Object> udfDataModel;
    
    private Properties props;
    
    @Override
    public void init(final Properties props) {
        this.props = props;
        // TODO fix ff3_1 maximum input length is 56 exception
        fpeMode = createFPEMode(props);
        fpeKey = createFPEKey(props);
        alphabetMapper = createAlphabetMapper(props);
        // TODO enhance tweak use in fpe
        fpeTweak = createFPETweak(props, fpeMode);
        fpeCipher = createFPECipher(props);
        udfDataModel = createUdfDataModel();
    }
    
    private String createFPEMode(final Properties props) {
        String result = props.getProperty(FPE_MODE, FF1_MODE).toUpperCase();
        ShardingSpherePreconditions.checkContains(VALID_FPE_MODES, result, () -> new AlgorithmInitializationException(this, "%s must be either %s or %s", FPE_MODE, FF1_MODE, FF3_1_MODE));
        return result;
    }
    
    private byte[] createFPEKey(final Properties props) {
        ShardingSpherePreconditions.checkContainsKey(props, FPE_KEY, () -> new AlgorithmInitializationException(this, "%s can not be null", FPE_KEY));
        byte[] result = props.getProperty(FPE_KEY).getBytes(StandardCharsets.UTF_8);
        ShardingSpherePreconditions.checkContains(VALID_FPE_KEY_LENGTHS, result.length * BYTE_BIT_SIZE,
                () -> new AlgorithmInitializationException(this, "Key length has to be either 128 or 192 or 256 bits."));
        return result;
    }
    
    private AlphabetMapper createAlphabetMapper(final Properties props) {
        ShardingSpherePreconditions.checkContainsKey(props, FPE_ALPHABET, () -> new AlgorithmInitializationException(this, "%s can not be null", FPE_ALPHABET));
        return new BasicAlphabetMapper(props.getProperty(FPE_ALPHABET));
    }
    
    private byte[] createFPETweak(final Properties props, final String fpeMode) {
        if (props.contains(FPE_TWEAK)) {
            return props.getProperty(FPE_TWEAK).getBytes(StandardCharsets.UTF_8);
        }
        return FF3_1_MODE.equalsIgnoreCase(fpeMode) ? new byte[7] : new byte[0];
    }
    
    private String createFPECipher(final Properties props) {
        String result = props.getProperty(FPE_CIPHER, "AES").toUpperCase();
        ShardingSpherePreconditions.checkState(VALID_FPE_CIPHERS.contains(result), () -> new AlgorithmInitializationException(this, "%s must be either AES or SM4", FPE_MODE));
        return result;
    }
    
    private Map<String, Object> createUdfDataModel() {
        // TODO support fpe for oracle udf, now oracle udf use bcprov-jdk15on 1.60, this version does not support fpe
        Map<String, Object> result = new HashMap<>(5, 1F);
        result.put("fpeMode", fpeMode);
        result.put("fpeKey", fpeKey);
        result.put("alphabetMapper", alphabetMapper);
        result.put("fpeTweak", fpeTweak);
        result.put("fpeCipher", fpeCipher);
        return result;
    }
    
    @Override
    public String encrypt(final Object plainValue) {
        return null == plainValue ? null : new String(encrypt(String.valueOf(plainValue).toCharArray()));
    }
    
    private char[] encrypt(final char[] plainValue) {
        return alphabetMapper.convertToChars(handle(alphabetMapper.convertToIndexes(plainValue), Cipher.ENCRYPT_MODE));
    }
    
    @Override
    public Object decrypt(final Object cipherValue) {
        return null == cipherValue ? null : new String(decrypt(cipherValue.toString().toCharArray()));
    }
    
    private char[] decrypt(final char[] cipherValue) {
        return alphabetMapper.convertToChars(handle(alphabetMapper.convertToIndexes(cipherValue), Cipher.DECRYPT_MODE));
    }
    
    @SneakyThrows(GeneralSecurityException.class)
    private byte[] handle(final byte[] input, final int mode) {
        boolean encryptMode = Cipher.ENCRYPT_MODE == mode;
        Cipher cipher = CipherInstanceManager.getInstance().getCipher(toConfiguration(), Cipher.class, () -> createCipherInstance(encryptMode), encryptMode);
        return cipher.doFinal(input);
    }
    
    @Override
    public AlgorithmConfiguration toConfiguration() {
        return new AlgorithmConfiguration(getType(), props);
    }
    
    private Cipher createCipherInstance(final boolean encryptMode) {
        return encryptMode ? getCipher(Cipher.ENCRYPT_MODE) : getCipher(Cipher.DECRYPT_MODE);
    }
    
    @SneakyThrows(GeneralSecurityException.class)
    private Cipher getCipher(final int encryptMode) {
        String transformation = String.format("%s/%s/NoPadding", fpeCipher, fpeMode);
        Cipher result = Cipher.getInstance(transformation, BouncyCastleProvider.PROVIDER_NAME);
        result.init(encryptMode, new SecretKeySpec(fpeKey, fpeMode), new FPEParameterSpec(alphabetMapper.getRadix(), fpeTweak));
        return result;
    }
    
    @Override
    public String getType() {
        return "SphereEx:FPE";
    }
}
