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

package com.sphereex.dbplusengine.encrypt.yaml.config;

import com.sphereex.dbplusengine.encrypt.config.rule.compatible.CompatibleEncryptRuleConfiguration;
import com.sphereex.dbplusengine.encrypt.yaml.config.rule.YamlCompatibleEncryptTableRuleConfiguration;
import com.sphereex.dbplusengine.encrypt.yaml.config.rule.YamlEncryptModeRuleConfiguration;
import lombok.Getter;
import lombok.Setter;
import org.apache.shardingsphere.infra.algorithm.core.yaml.YamlAlgorithmConfiguration;
import org.apache.shardingsphere.infra.yaml.config.pojo.rule.YamlRuleConfiguration;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Compatible encrypt rule configuration for YAML.
 */
@Getter
@Setter
public final class YamlCompatibleEncryptRuleConfiguration implements YamlRuleConfiguration {
    
    private Map<String, YamlCompatibleEncryptTableRuleConfiguration> tables = new LinkedHashMap<>();
    
    private Map<String, YamlAlgorithmConfiguration> encryptors = new LinkedHashMap<>();
    
    private YamlEncryptModeRuleConfiguration encryptMode;
    
    private boolean queryWithCipherColumn = true;
    
    @Override
    public Class<CompatibleEncryptRuleConfiguration> getRuleConfigurationType() {
        return CompatibleEncryptRuleConfiguration.class;
    }
}
