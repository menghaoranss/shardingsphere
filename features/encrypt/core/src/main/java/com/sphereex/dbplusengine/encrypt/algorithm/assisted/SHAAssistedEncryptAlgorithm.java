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

package com.sphereex.dbplusengine.encrypt.algorithm.assisted;

import com.sphereex.dbplusengine.encrypt.context.EncryptContext;
import lombok.Getter;
import org.apache.shardingsphere.encrypt.spi.EncryptAlgorithm;
import org.apache.shardingsphere.encrypt.spi.EncryptAlgorithmMetaData;
import org.apache.shardingsphere.infra.algorithm.core.config.AlgorithmConfiguration;
import org.apache.shardingsphere.infra.algorithm.core.context.AlgorithmSQLContext;
import org.apache.shardingsphere.infra.algorithm.messagedigest.core.MessageDigestAlgorithm;
import org.apache.shardingsphere.infra.spi.type.typed.TypedSPILoader;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;

/**
 * SHA assisted encrypt algorithm.
 */
@Getter
public final class SHAAssistedEncryptAlgorithm implements EncryptAlgorithm {
    
    private final EncryptAlgorithmMetaData metaData = new EncryptAlgorithmMetaData(false, true, false, false, this::calculateExpansibility);
    
    private Properties props;
    
    private MessageDigestAlgorithm digestAlgorithm;
    
    private Map<String, Object> udfDataModel;
    
    @Override
    public void init(final Properties props) {
        this.props = props;
        digestAlgorithm = TypedSPILoader.getService(MessageDigestAlgorithm.class, getType(), props);
        udfDataModel = Collections.emptyMap();
    }
    
    @Override
    public String encrypt(final Object plainValue, final AlgorithmSQLContext algorithmSQLContext, final EncryptContext encryptContext) {
        return digestAlgorithm.digest(plainValue);
    }
    
    @Override
    public Object decrypt(final Object cipherValue, final AlgorithmSQLContext algorithmSQLContext, final EncryptContext encryptContext) {
        throw new UnsupportedOperationException(String.format("Algorithm `%s` is unsupported to decrypt", getType()));
    }
    
    @Override
    public AlgorithmConfiguration toConfiguration() {
        return new AlgorithmConfiguration(getType(), props);
    }
    
    private int calculateExpansibility(final int plainTextCharLength, final int charToByteRatio) {
        int cipherByteLength = 32;
        return 4 * ((cipherByteLength + 2) / 3);
    }
    
    @Override
    public String getType() {
        return "SphereEx:SHA";
    }
}
