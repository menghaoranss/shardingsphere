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

package com.sphereex.dbplusengine.infra.algorithm.cryptographic.aes.props;

import lombok.Getter;
import org.apache.shardingsphere.infra.algorithm.cryptographic.core.CryptographicPropertiesProvider;

import java.util.Arrays;
import java.util.Collection;
import java.util.Properties;

/**
 * MariaDB AES properties provider.
 */
public final class MariaDBAESPropertiesProvider extends AbstractMySQLTrunkTypeAESPropertiesProvider implements CryptographicPropertiesProvider {
    
    private static final Collection<String> SUPPORTED_MODES = Arrays.asList("ECB", "CBC", "CTR");
    
    @Getter
    private byte[] secretKey;
    
    @Getter
    private String mode;
    
    @Getter
    private String padding;
    
    @Getter
    private byte[] ivParameter;
    
    @Getter
    private String encoder;
    
    @Override
    public void init(final Properties props) {
        int secretKeyByteLength = getKeyBitLength(props) / 8;
        secretKey = getSecretKey(props, secretKeyByteLength);
        mode = getMode(props);
        ivParameter = getIvParameter(props);
        padding = getPadding(mode);
        encoder = getEncoder(props);
    }
    
    @Override
    protected Collection<String> getSupportedModes() {
        return SUPPORTED_MODES;
    }
    
    @Override
    public String getType() {
        return "MariaDB";
    }
}
