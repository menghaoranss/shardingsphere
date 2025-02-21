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

import lombok.Getter;
import lombok.SneakyThrows;
import org.apache.shardingsphere.infra.algorithm.core.config.AlgorithmConfiguration;
import org.apache.shardingsphere.infra.algorithm.core.exception.AlgorithmInitializationException;
import org.apache.shardingsphere.infra.algorithm.cryptographic.core.CryptographicAlgorithm;
import org.apache.shardingsphere.infra.exception.core.ShardingSpherePreconditions;
import org.bouncycastle.asn1.gm.GMNamedCurves;
import org.bouncycastle.asn1.x9.X9ECParameters;
import org.bouncycastle.crypto.InvalidCipherTextException;
import org.bouncycastle.crypto.engines.SM2Engine;
import org.bouncycastle.crypto.params.ECDomainParameters;
import org.bouncycastle.crypto.params.ECPrivateKeyParameters;
import org.bouncycastle.crypto.params.ECPublicKeyParameters;
import org.bouncycastle.crypto.params.ParametersWithRandom;
import org.bouncycastle.jcajce.provider.asymmetric.ec.BCECPrivateKey;
import org.bouncycastle.jcajce.provider.asymmetric.ec.BCECPublicKey;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.jce.spec.ECParameterSpec;
import org.bouncycastle.util.BigIntegers;

import java.nio.charset.StandardCharsets;
import java.security.KeyFactory;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.SecureRandom;
import java.security.Security;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.PKCS8EncodedKeySpec;
import java.security.spec.X509EncodedKeySpec;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * SM2 cryptographic algorithm.
 */
public final class SM2CryptographicAlgorithm implements CryptographicAlgorithm {
    
    private static final String SM2_PUBLIC_KEY = "sm2-public-key-value";
    
    private static final String SM2_PRIVATE_KEY = "sm2-private-key-value";
    
    private static final String ELLIPTIC_CURVE_ALGORITHM = "EC";
    
    private static final String SM2_CURVE_NAME = "sm2p256v1";
    
    private ECPublicKeyParameters publicKeyParams;
    
    private ECPrivateKeyParameters privateKeyParams;
    
    @Getter
    private Map<String, Object> udfDataModel;
    
    private Properties props;
    
    static {
        Security.addProvider(new BouncyCastleProvider());
    }
    
    @Override
    public void init(final Properties props) {
        this.props = props;
        publicKeyParams = createPublicKeyParameters(props);
        privateKeyParams = createPrivateKeyParameters(props);
        udfDataModel = createUdfDataModel(props);
    }
    
    private Map<String, Object> createUdfDataModel(final Properties props) {
        Map<String, Object> result = new HashMap<>(2, 1F);
        result.put("sm2PublicKey", props.getProperty(SM2_PUBLIC_KEY));
        result.put("sm2PrivateKey", props.getProperty(SM2_PRIVATE_KEY));
        return result;
    }
    
    @Override
    public String encrypt(final Object plainValue) {
        return null == plainValue ? null : Base64.getEncoder().encodeToString(encrypt(String.valueOf(plainValue).getBytes(StandardCharsets.UTF_8)));
    }
    
    @SneakyThrows(InvalidCipherTextException.class)
    private byte[] encrypt(final byte[] input) {
        SM2Engine sm2Engine = new SM2Engine();
        sm2Engine.init(true, new ParametersWithRandom(publicKeyParams, new SecureRandom()));
        return sm2Engine.processBlock(input, 0, input.length);
    }
    
    @Override
    public Object decrypt(final Object cipherValue) {
        return null == cipherValue ? null : new String(decrypt(Base64.getDecoder().decode(cipherValue.toString())), StandardCharsets.UTF_8);
    }
    
    @SneakyThrows(InvalidCipherTextException.class)
    private byte[] decrypt(final byte[] input) {
        SM2Engine sm2Engine = new SM2Engine();
        sm2Engine.init(false, privateKeyParams);
        return sm2Engine.processBlock(input, 0, input.length);
    }
    
    private ECPublicKeyParameters createPublicKeyParameters(final BCECPublicKey bcecPublicKey) {
        ECParameterSpec params = bcecPublicKey.getParameters();
        return new ECPublicKeyParameters(bcecPublicKey.getQ(), new ECDomainParameters(params.getCurve(), params.getG(), params.getN()));
    }
    
    private ECPublicKeyParameters createPublicKeyParameters(final Properties props) {
        ShardingSpherePreconditions.checkNotNull(props.getProperty(SM2_PUBLIC_KEY), () -> new AlgorithmInitializationException(this, "%s can not be null", SM2_PUBLIC_KEY));
        return createPublicKeyParameters(props.getProperty(SM2_PUBLIC_KEY));
    }
    
    private ECPublicKeyParameters createPublicKeyParameters(final String publicKey) {
        byte[] key = base64Decode(publicKey, true);
        try {
            return createPublicKeyParametersByCurve(key);
            // CHECKSTYLE:OFF
        } catch (final Exception ignored) {
            // CHECKSTYLE:ON
        }
        return createPublicKeyParameters((BCECPublicKey) getPublicKey(key));
    }
    
    private ECPublicKeyParameters createPublicKeyParametersByCurve(final byte[] key) {
        X9ECParameters x9ECParameters = GMNamedCurves.getByName(SM2_CURVE_NAME);
        ECDomainParameters domainParameters = new ECDomainParameters(x9ECParameters.getCurve(), x9ECParameters.getG(), x9ECParameters.getN(), x9ECParameters.getH());
        return new ECPublicKeyParameters(domainParameters.getCurve().decodePoint(key), domainParameters);
    }
    
    private PublicKey getPublicKey(final byte[] key) {
        try {
            return KeyFactory.getInstance(ELLIPTIC_CURVE_ALGORITHM, BouncyCastleProvider.PROVIDER_NAME).generatePublic(new X509EncodedKeySpec(key));
        } catch (final InvalidKeySpecException | NoSuchAlgorithmException | NoSuchProviderException ex) {
            throw new AlgorithmInitializationException(this, "can not generate public key, " + ex.getMessage());
        }
    }
    
    private ECPrivateKeyParameters createPrivateKeyParameters(final BCECPrivateKey bcecPrivateKey) {
        ECParameterSpec params = bcecPrivateKey.getParameters();
        return new ECPrivateKeyParameters(bcecPrivateKey.getD(), new ECDomainParameters(params.getCurve(), params.getG(), params.getN()));
    }
    
    private ECPrivateKeyParameters createPrivateKeyParameters(final Properties props) {
        ShardingSpherePreconditions.checkNotNull(props.getProperty(SM2_PRIVATE_KEY), () -> new AlgorithmInitializationException(this, String.format("%s can not be null", SM2_PRIVATE_KEY)));
        return createPrivateKeyParameters(props.getProperty(SM2_PRIVATE_KEY));
    }
    
    private ECPrivateKeyParameters createPrivateKeyParameters(final String privateKey) {
        byte[] key = base64Decode(privateKey, false);
        try {
            return createPrivateParamByCurve(key);
            // CHECKSTYLE:OFF
        } catch (final Exception ignored) {
            // CHECKSTYLE:ON
        }
        try {
            return createPrivateKeyParameters((BCECPrivateKey) getPrivateKey(key));
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
    
    private PrivateKey getPrivateKey(final byte[] key) throws InvalidKeySpecException, NoSuchAlgorithmException, NoSuchProviderException {
        return KeyFactory.getInstance(ELLIPTIC_CURVE_ALGORITHM, BouncyCastleProvider.PROVIDER_NAME).generatePrivate(new PKCS8EncodedKeySpec(key));
    }
    
    private ECPrivateKeyParameters createPrivateParamByCurve(final byte[] privateKey) {
        X9ECParameters x9ECParameters = GMNamedCurves.getByName(SM2_CURVE_NAME);
        ECDomainParameters domainParameters = new ECDomainParameters(x9ECParameters.getCurve(), x9ECParameters.getG(), x9ECParameters.getN(), x9ECParameters.getH());
        return new ECPrivateKeyParameters(BigIntegers.fromUnsignedByteArray(privateKey), domainParameters);
    }
    
    @Override
    public AlgorithmConfiguration toConfiguration() {
        return new AlgorithmConfiguration(getType(), props);
    }
    
    @Override
    public String getType() {
        return "SphereEx:SM2";
    }
}
