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

package com.sphereex.dbplusengine.infra.algorithm.messagedigest.sha;

import com.sphereex.dbplusengine.SphereEx;
import com.sphereex.dbplusengine.infra.expansibility.ExpansibilityProvider;
import lombok.SneakyThrows;
import org.apache.shardingsphere.infra.algorithm.messagedigest.core.MessageDigestAlgorithm;

import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.MessageDigest;
import java.util.Base64;

/**
 * SHA message digest algorithm.
 */
public final class SHAMessageDigestAlgorithm implements MessageDigestAlgorithm, @SphereEx ExpansibilityProvider {
    
    private static final String SHA_256_ALGORITHM_NAME = "SHA-256";
    
    @SneakyThrows(GeneralSecurityException.class)
    @Override
    public String digest(final Object plainValue) {
        if (null == plainValue) {
            return null;
        }
        MessageDigest messageDigest = MessageDigest.getInstance(SHA_256_ALGORITHM_NAME);
        byte[] result = messageDigest.digest(plainValue.toString().getBytes(StandardCharsets.UTF_8));
        return Base64.getEncoder().encodeToString(result);
    }
    
    @Override
    public String getType() {
        return "SphereEx:SHA";
    }
    
    @SphereEx
    @Override
    public int calculate(final int dataCharLength, final int charToByteRatio) {
        int digestByteLength = 32;
        return 4 * ((digestByteLength + 2) / 3);
    }
}
