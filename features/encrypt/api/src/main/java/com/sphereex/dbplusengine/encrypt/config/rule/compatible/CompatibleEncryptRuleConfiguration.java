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

package com.sphereex.dbplusengine.encrypt.config.rule.compatible;

import com.sphereex.dbplusengine.SphereEx;
import com.sphereex.dbplusengine.encrypt.config.rule.mode.EncryptModeRuleConfiguration;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import org.apache.shardingsphere.encrypt.config.EncryptRuleConfiguration;
import org.apache.shardingsphere.encrypt.config.rule.EncryptTableRuleConfiguration;
import org.apache.shardingsphere.infra.algorithm.core.config.AlgorithmConfiguration;
import org.apache.shardingsphere.infra.config.rule.function.EnhancedRuleConfiguration;
import org.apache.shardingsphere.infra.config.rule.scope.DatabaseRuleConfiguration;

import java.util.Collection;
import java.util.LinkedList;
import java.util.Map;

/**
 * Compatible encrypt rule configuration.
 */
@RequiredArgsConstructor
@Getter
public final class CompatibleEncryptRuleConfiguration implements DatabaseRuleConfiguration, EnhancedRuleConfiguration {
    
    private final Collection<CompatibleEncryptTableRuleConfiguration> tables;
    
    private final Map<String, AlgorithmConfiguration> encryptors;
    
    private final boolean queryWithCipherColumn;
    
    @SphereEx
    @Setter
    private EncryptModeRuleConfiguration encryptMode;
    
    /**
     * Convert to encrypt rule configuration.
     *
     * @return encrypt rule configuration
     */
    public EncryptRuleConfiguration toEncryptRuleConfig() {
        Collection<EncryptTableRuleConfiguration> tables = new LinkedList<>();
        this.tables.forEach(each -> tables.add(each.toEncryptTableRuleConfig(queryWithCipherColumn)));
        EncryptRuleConfiguration result = new EncryptRuleConfiguration(tables, encryptors);
        result.setEncryptMode(encryptMode);
        return result;
    }
}
