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

package com.sphereex.dbplusengine.encrypt.yaml.swapper.rule;

import com.sphereex.dbplusengine.encrypt.config.rule.mode.EncryptModeRuleConfiguration;
import com.sphereex.dbplusengine.encrypt.config.rule.mode.EncryptModeType;
import com.sphereex.dbplusengine.encrypt.yaml.config.rule.YamlEncryptModeRuleConfiguration;
import org.apache.shardingsphere.infra.util.yaml.swapper.YamlConfigurationSwapper;

/**
 * YAML encrypt mode rule configuration swapper.
 */
public final class YamlEncryptModeRuleConfigurationSwapper implements YamlConfigurationSwapper<YamlEncryptModeRuleConfiguration, EncryptModeRuleConfiguration> {
    
    @Override
    public YamlEncryptModeRuleConfiguration swapToYamlConfiguration(final EncryptModeRuleConfiguration data) {
        YamlEncryptModeRuleConfiguration result = new YamlEncryptModeRuleConfiguration();
        result.setType(data.getType().name());
        result.setProps(data.getProps());
        return result;
    }
    
    @Override
    public EncryptModeRuleConfiguration swapToObject(final YamlEncryptModeRuleConfiguration yamlConfig) {
        return new EncryptModeRuleConfiguration(EncryptModeType.valueOf(yamlConfig.getType()), yamlConfig.getProps());
    }
}
