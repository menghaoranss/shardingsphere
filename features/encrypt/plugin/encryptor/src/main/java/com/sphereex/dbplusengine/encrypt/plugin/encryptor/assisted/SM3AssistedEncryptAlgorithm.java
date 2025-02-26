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

package com.sphereex.dbplusengine.encrypt.plugin.encryptor.assisted;

import com.sphereex.dbplusengine.SphereEx;
import com.sphereex.dbplusengine.SphereEx.Type;
import com.sphereex.dbplusengine.encrypt.context.EncryptContext;
import lombok.Getter;
import org.apache.shardingsphere.encrypt.spi.EncryptAlgorithm;
import org.apache.shardingsphere.encrypt.spi.EncryptAlgorithmMetaData;
import org.apache.shardingsphere.infra.algorithm.core.config.AlgorithmConfiguration;
import org.apache.shardingsphere.infra.algorithm.core.context.AlgorithmSQLContext;
import org.apache.shardingsphere.infra.algorithm.messagedigest.core.MessageDigestAlgorithm;
import org.apache.shardingsphere.infra.spi.type.typed.TypedSPILoader;
import org.bouncycastle.jce.provider.BouncyCastleProvider;

import java.security.Security;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;

/**
 * SM3 assisted encrypt algorithm.
 */
@SphereEx(Type.COPY)
public final class SM3AssistedEncryptAlgorithm implements EncryptAlgorithm {
    
    @SphereEx
    private static final String SM3_SALT = "sm3-salt";
    
    @SphereEx(Type.MODIFY)
    @Getter
    private final EncryptAlgorithmMetaData metaData = new EncryptAlgorithmMetaData(false, true, false, false, (plainCharLength, charToByteRatio) -> 32 << 1);
    
    private Properties props;
    
    private MessageDigestAlgorithm digestAlgorithm;
    
    @SphereEx
    @Getter
    private Map<String, Object> udfDataModel;
    
    static {
        Security.addProvider(new BouncyCastleProvider());
    }
    
    @Override
    public void init(final Properties props) {
        this.props = props;
        digestAlgorithm = TypedSPILoader.getService(MessageDigestAlgorithm.class, getType(), props);
        // SPEX ADDED: BEGIN
        udfDataModel = createUdfDataModel(props);
        // SPEX ADDED: END
    }
    
    @Override
    public String encrypt(final Object plainValue, final AlgorithmSQLContext algorithmSQLContext, @SphereEx final EncryptContext encryptContext) {
        return digestAlgorithm.digest(plainValue);
    }
    
    @Override
    public Object decrypt(final Object cipherValue, final AlgorithmSQLContext algorithmSQLContext, @SphereEx final EncryptContext encryptContext) {
        throw new UnsupportedOperationException(String.format("Algorithm `%s` is unsupported to decrypt", getType()));
    }
    
    @SphereEx
    private Map<String, Object> createUdfDataModel(final Properties props) {
        return Collections.singletonMap("sm3Salt", props.getProperty(SM3_SALT, ""));
    }
    
    @Override
    public AlgorithmConfiguration toConfiguration() {
        return new AlgorithmConfiguration(getType(), props);
    }
    
    @Override
    public String getType() {
        return "SM3";
    }
}
