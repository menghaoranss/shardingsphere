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

package com.sphereex.dbplusengine.infra.algorithm.cryptographic.aes;

import org.apache.shardingsphere.infra.algorithm.cryptographic.core.CryptographicAlgorithm;
import org.apache.shardingsphere.infra.spi.type.typed.TypedSPILoader;
import org.apache.shardingsphere.test.util.PropertiesBuilder;
import org.apache.shardingsphere.test.util.PropertiesBuilder.Property;
import org.junit.jupiter.api.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

class OceanBaseOracleModeAESCryptographicAlgorithmTest {
    
    private static final String OCEAN_BASE = "OceanBase_Oracle";
    
    @Test
    void assertOceanBaseEncrypt() {
        CryptographicAlgorithm oceanBaseAlgorithm = TypedSPILoader.getService(CryptographicAlgorithm.class, "AES",
                PropertiesBuilder.build(new Property("db-compatible-mode", OCEAN_BASE), new Property("aes-key-value", "test")));
        assertEncryptAndDecrypt(oceanBaseAlgorithm, "test", "205421D0F2D617096DBE447338A44DF9");
        assertEncryptAndDecrypt(oceanBaseAlgorithm, "testLongLongLongLongLongLongText", "91385FDEF70AF8B7A8037921D480E1C98A8344F6B07CD57594781C11E8C539E0B380435148E09C970C3C78469C5E8F0C");
        oceanBaseAlgorithm = TypedSPILoader.getService(CryptographicAlgorithm.class, "AES",
                PropertiesBuilder.build(new Property("db-compatible-mode", OCEAN_BASE), new Property("aes-key-value", "test_key")));
        assertEncryptAndDecrypt(oceanBaseAlgorithm, "test", "DF0FE75CDCB5F5828048556DC69E18FA");
        assertEncryptAndDecrypt(oceanBaseAlgorithm, "testLongLongLongLongLongLongText", "58D4C15ADEE2A155ADB602749C461FC874B1DBA9435171B175E0B141C453F160420B2333AE68A47C485DB8A008675181");
        oceanBaseAlgorithm = TypedSPILoader.getService(CryptographicAlgorithm.class, "AES",
                PropertiesBuilder.build(new Property("db-compatible-mode", OCEAN_BASE), new Property("aes-key-value", "test_long_long_long_long_long_long_long_long_long_long_long_long_long_key")));
        assertEncryptAndDecrypt(oceanBaseAlgorithm, "test", "A831F50AD46CDAB55379A354B167FA0F");
        assertEncryptAndDecrypt(oceanBaseAlgorithm, "testLongLongLongLongLongLongText", "748F6747F940FE81AC84A13873B64822C145BEBA9CFA75B81F12EEAC99FCBC3B1C67C6AA032C329ECBFCA299DE0CB26A");
    }
    
    @Test
    void assertOceanBaseEncryptWithBase64Encode() {
        CryptographicAlgorithm oceanBaseAlgorithm = TypedSPILoader.getService(CryptographicAlgorithm.class, "AES",
                PropertiesBuilder.build(new Property("db-compatible-mode", OCEAN_BASE), new Property("aes-key-value", "test"), new Property("aes-encoder", "BASE64")));
        assertEncryptAndDecrypt(oceanBaseAlgorithm, "test", "IFQh0PLWFwltvkRzOKRN+Q==");
        assertEncryptAndDecrypt(oceanBaseAlgorithm, "testLongLongLongLongLongLongText", "kThf3vcK+LeoA3kh1IDhyYqDRPawfNV1lHgcEejFOeCzgENRSOCclww8eEacXo8M");
    }
    
    private void assertEncryptAndDecrypt(final CryptographicAlgorithm oceanBaseAlgorithm, final String plainValue, final String encryptedValue) {
        String encryptedText = oceanBaseAlgorithm.encrypt(plainValue).toString();
        assertThat(encryptedText, is(encryptedValue));
        assertThat(oceanBaseAlgorithm.decrypt(encryptedText), is(plainValue));
    }
}
