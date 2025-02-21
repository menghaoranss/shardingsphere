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

package org.apache.shardingsphere.test.it.rewrite.fixture.encrypt;

import com.sphereex.dbplusengine.SphereEx;
import com.sphereex.dbplusengine.SphereEx.Type;
import com.sphereex.dbplusengine.encrypt.context.EncryptContext;
import lombok.Getter;
import org.apache.shardingsphere.encrypt.spi.EncryptAlgorithm;
import org.apache.shardingsphere.encrypt.spi.EncryptAlgorithmMetaData;
import org.apache.shardingsphere.infra.algorithm.core.config.AlgorithmConfiguration;
import org.apache.shardingsphere.infra.algorithm.core.context.AlgorithmSQLContext;

import java.util.Collections;
import java.util.Map;

import java.util.Properties;

@Getter
public final class RewriteNormalEncryptAlgorithmFixture implements EncryptAlgorithm {
    
    @SphereEx(Type.MODIFY)
    private final EncryptAlgorithmMetaData metaData = new EncryptAlgorithmMetaData(true, true, false, false, (plainCharLength, charToByteRatio) -> 2 * plainCharLength);
    
    @Override
    public String encrypt(final Object plainValue, final AlgorithmSQLContext algorithmSQLContext, @SphereEx final EncryptContext encryptContext) {
        if (null == plainValue) {
            return null;
        }
        return "encrypt_" + plainValue;
    }
    
    @Override
    public Object decrypt(final Object cipherValue, final AlgorithmSQLContext algorithmSQLContext, @SphereEx final EncryptContext encryptContext) {
        if (null == cipherValue) {
            return null;
        }
        return cipherValue.toString().replaceAll("encrypt_", "");
    }
    
    @SphereEx
    @Override
    public Map<String, Object> getUdfDataModel() {
        return Collections.emptyMap();
    }
    
    @Override
    public AlgorithmConfiguration toConfiguration() {
        return new AlgorithmConfiguration(getType(), new Properties());
    }
    
    @Override
    public String getType() {
        return "REWRITE.NORMAL.FIXTURE";
    }
}
