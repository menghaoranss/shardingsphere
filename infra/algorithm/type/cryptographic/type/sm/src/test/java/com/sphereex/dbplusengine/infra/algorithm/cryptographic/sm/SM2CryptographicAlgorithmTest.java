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
import org.apache.shardingsphere.infra.algorithm.core.exception.AlgorithmInitializationException;
import org.apache.shardingsphere.infra.algorithm.cryptographic.core.CryptographicAlgorithm;
import org.apache.shardingsphere.infra.spi.type.typed.TypedSPILoader;
import org.apache.shardingsphere.test.util.PropertiesBuilder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class SM2CryptographicAlgorithmTest {
    
    private static final String SM2 = "SphereEx:SM2";
    
    private static final String SM2_PUBLIC_KEY = "MFkwEwYHKoZIzj0CAQYIKoEcz1UBgi0DQgAE0oppHTfuiESO0DR+9c5g7iRlrbDHgPVeRQzNsskL4ZSHkYvyms76Zv4He95WySnTuZMo0OaQchhRbmXIkXRuyA==";
    
    private static final String SM2_PRIVATE_KEY = getPrivateKey();
    
    private CryptographicAlgorithm encryptAlgorithm;
    
    @BeforeEach
    void setUp() {
        encryptAlgorithm = TypedSPILoader.getService(CryptographicAlgorithm.class, SM2, getProps());
    }
    
    private static Properties getProps() {
        return PropertiesBuilder.build(new PropertiesBuilder.Property("sm2-public-key-value", SM2_PUBLIC_KEY), new PropertiesBuilder.Property("sm2-private-key-value", SM2_PRIVATE_KEY));
    }
    
    @SphereEx(Type.MODIFY)
    @Test
    void assertEncryptNullValue() {
        assertNull(encryptAlgorithm.encrypt(null));
    }
    
    @SphereEx(Type.MODIFY)
    @Test
    void assertEncrypt() {
        Object encryptedValue = encryptAlgorithm.encrypt("test");
        Object encryptedValue2 = encryptAlgorithm.encrypt("test");
        assertThat(encryptedValue.toString(), not("test"));
        assertThat(encryptedValue.toString(), not(encryptedValue2.toString()));
    }
    
    @Test
    void assertEncryptAndDecrypt() {
        encryptAndDecrypt(encryptAlgorithm, "test");
        encryptAndDecrypt(encryptAlgorithm, "testSM2");
        encryptAndDecrypt(encryptAlgorithm, "testSM2中文");
        encryptAndDecrypt(encryptAlgorithm, "testSM2中文?&!*@#");
    }
    
    @Test
    void assertIllegalKeys() {
        String publicKey = "1";
        String privateKey = "1";
        assertThrows(AlgorithmInitializationException.class, () -> TypedSPILoader.getService(CryptographicAlgorithm.class, SM2,
                PropertiesBuilder.build(new PropertiesBuilder.Property("sm2-public-key-value", publicKey), new PropertiesBuilder.Property("sm2-private-key-value", privateKey))));
    }
    
    @Test
    void assertEncryptAndDecryptByCurveKey() {
        String publicKey = "BFUILFsKefapGITBqH5pj00QSoKLt0EVS62WFarauvbLzBXIxHSGgAO8QAvrLsMOsy7q99bNrh1q7G/GD3+7du8=";
        String privateKey = "ALrrsyAtus5SXWCyP8xSsjCOXFTfgcwGCJA/knK1lYTs";
        CryptographicAlgorithm algorithm = TypedSPILoader.getService(CryptographicAlgorithm.class, SM2,
                PropertiesBuilder.build(new PropertiesBuilder.Property("sm2-public-key-value", publicKey), new PropertiesBuilder.Property("sm2-private-key-value", privateKey)));
        encryptAndDecrypt(algorithm, "testSM2中文?&!*@#");
    }
    
    @SphereEx(Type.MODIFY)
    private void encryptAndDecrypt(final CryptographicAlgorithm algorithm, final String plainValue) {
        Object encryptedValue = algorithm.encrypt(plainValue);
        Object decryptedValue = algorithm.decrypt(encryptedValue);
        assertThat(decryptedValue.toString(), is(plainValue));
    }
    
    private static String getPrivateKey() {
        return "MIGTAgEAMBMGByqGSM49AgEGCCqBHM9VAYItBHkwdwIBAQQg7ltTxwCxo5gUftPXTLCfDCKCvl7284CRkc/bk4YyzJagCgYIKoEcz1UB"
                + "gi2hRANCAATSimkdN+6IRI7QNH71zmDuJGWtsMeA9V5FDM2yyQvhlIeRi/Kazvpm/gd73lbJKdO5kyjQ5pByGFFuZciRdG7I";
    }
}
