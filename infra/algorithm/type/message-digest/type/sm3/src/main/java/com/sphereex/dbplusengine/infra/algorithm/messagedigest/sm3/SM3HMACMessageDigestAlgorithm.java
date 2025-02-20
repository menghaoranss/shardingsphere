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

package com.sphereex.dbplusengine.infra.algorithm.messagedigest.sm3;

import com.sphereex.dbplusengine.infra.algorithm.messagedigest.core.HMACMessageDigestAlgorithm;
import com.sphereex.dbplusengine.infra.expansibility.ExpansibilityProvider;
import lombok.SneakyThrows;
import org.apache.commons.codec.binary.Hex;
import org.apache.shardingsphere.infra.algorithm.core.exception.AlgorithmInitializationException;
import org.apache.shardingsphere.infra.exception.core.ShardingSpherePreconditions;
import org.bouncycastle.jce.provider.BouncyCastleProvider;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.Security;
import java.util.Properties;

/**
 * SM3 HMAC message digest algorithm.
 */
public final class SM3HMACMessageDigestAlgorithm implements HMACMessageDigestAlgorithm, ExpansibilityProvider {
    
    private static final String HMAC_SM3_SALT = "hmac-sm3-salt";
    
    private static final String HMAC_SM3 = "HMACSM3";
    
    private byte[] sm3Salt;
    
    private Mac mac;
    
    static {
        Security.addProvider(new BouncyCastleProvider());
    }
    
    @SneakyThrows({NoSuchAlgorithmException.class, NoSuchProviderException.class, InvalidKeyException.class})
    @Override
    public void init(final Properties props) {
        sm3Salt = getSalt(props);
        mac = Mac.getInstance(HMAC_SM3, "BC");
        mac.init(new SecretKeySpec(sm3Salt, HMAC_SM3));
    }
    
    private byte[] getSalt(final Properties props) {
        String salt = props.getProperty(HMAC_SM3_SALT, "");
        ShardingSpherePreconditions.checkState(!salt.isEmpty(), () -> new AlgorithmInitializationException(this, "hmac-sm3-salt must be provided"));
        return salt.getBytes(StandardCharsets.UTF_8);
    }
    
    @Override
    public String digest(final Object plainValue) {
        return null == plainValue ? null : Hex.encodeHexString(getHmacBytes(plainValue));
    }
    
    private byte[] getHmacBytes(final Object plainValue) {
        return mac.doFinal(String.valueOf(plainValue).getBytes(StandardCharsets.UTF_8));
    }
    
    @Override
    public String getType() {
        return "HMAC_SM3";
    }
    
    @Override
    public int calculate(final int dataCharLength, final int charToByteRatio) {
        return 32 << 1;
    }
}
