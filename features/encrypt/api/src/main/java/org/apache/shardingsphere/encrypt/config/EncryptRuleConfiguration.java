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

package org.apache.shardingsphere.encrypt.config;

import com.sphereex.dbplusengine.SphereEx;
import com.sphereex.dbplusengine.encrypt.config.rule.mode.EncryptModeRuleConfiguration;
import com.sphereex.dbplusengine.encrypt.config.rule.mode.EncryptModeType;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import org.apache.shardingsphere.encrypt.config.rule.EncryptTableRuleConfiguration;
import org.apache.shardingsphere.infra.algorithm.core.config.AlgorithmConfiguration;
import org.apache.shardingsphere.infra.config.rule.function.EnhancedRuleConfiguration;
import org.apache.shardingsphere.infra.config.rule.scope.DatabaseRuleConfiguration;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Encrypt rule configuration.
 */
@RequiredArgsConstructor
@Getter
public final class EncryptRuleConfiguration implements DatabaseRuleConfiguration, EnhancedRuleConfiguration {
    
    private final Collection<EncryptTableRuleConfiguration> tables;
    
    private final Map<String, AlgorithmConfiguration> encryptors;
    
    @SphereEx
    private final Map<String, AlgorithmConfiguration> keyManagers = new LinkedHashMap<>();
    
    @SphereEx
    @Setter
    private EncryptModeRuleConfiguration encryptMode;
    
    /**
     * Judge whether UDF routine is enabled.
     *
     * @return UDF routine is enabled or not
     */
    // TODO move this method to encrypt rule when distsql and pipeline job can use encrypt rule
    @SphereEx
    public boolean isUDFEnabled() {
        if (null == encryptMode) {
            return false;
        }
        if (EncryptModeType.BACKEND == encryptMode.getType()) {
            return true;
        }
        return Boolean.parseBoolean(encryptMode.getProps().getProperty("udf-sql-enabled", Boolean.FALSE.toString()))
                || Boolean.parseBoolean(encryptMode.getProps().getProperty("udf-routine-enabled", Boolean.FALSE.toString()))
                || Boolean.parseBoolean(encryptMode.getProps().getProperty("udf-view-enabled", Boolean.FALSE.toString()));
    }
}
