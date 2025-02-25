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

package com.sphereex.dbplusengine.encrypt.yaml.swapper;

import com.sphereex.dbplusengine.encrypt.config.rule.compatible.CompatibleEncryptRuleConfiguration;
import com.sphereex.dbplusengine.encrypt.config.rule.compatible.CompatibleEncryptTableRuleConfiguration;
import com.sphereex.dbplusengine.encrypt.yaml.config.YamlCompatibleEncryptRuleConfiguration;
import com.sphereex.dbplusengine.encrypt.yaml.config.rule.YamlCompatibleEncryptTableRuleConfiguration;
import com.sphereex.dbplusengine.encrypt.yaml.swapper.rule.YamlCompatibleEncryptTableRuleConfigurationSwapper;
import com.sphereex.dbplusengine.encrypt.yaml.swapper.rule.YamlEncryptModeRuleConfigurationSwapper;
import org.apache.shardingsphere.encrypt.constant.EncryptOrder;
import org.apache.shardingsphere.infra.algorithm.core.config.AlgorithmConfiguration;
import org.apache.shardingsphere.infra.algorithm.core.yaml.YamlAlgorithmConfiguration;
import org.apache.shardingsphere.infra.algorithm.core.yaml.YamlAlgorithmConfigurationSwapper;
import org.apache.shardingsphere.infra.yaml.config.swapper.rule.YamlRuleConfigurationSwapper;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Map.Entry;

/**
 * YAML compatible encrypt rule configuration swapper.
 */
public final class YamlCompatibleEncryptRuleConfigurationSwapper implements YamlRuleConfigurationSwapper<YamlCompatibleEncryptRuleConfiguration, CompatibleEncryptRuleConfiguration> {
    
    private final YamlCompatibleEncryptTableRuleConfigurationSwapper tableSwapper = new YamlCompatibleEncryptTableRuleConfigurationSwapper();
    
    private final YamlAlgorithmConfigurationSwapper algorithmSwapper = new YamlAlgorithmConfigurationSwapper();
    
    private final YamlEncryptModeRuleConfigurationSwapper encryptModeSwapper = new YamlEncryptModeRuleConfigurationSwapper();
    
    @Override
    public YamlCompatibleEncryptRuleConfiguration swapToYamlConfiguration(final CompatibleEncryptRuleConfiguration data) {
        YamlCompatibleEncryptRuleConfiguration result = new YamlCompatibleEncryptRuleConfiguration();
        data.getTables().forEach(each -> result.getTables().put(each.getName(), tableSwapper.swapToYamlConfiguration(each)));
        data.getEncryptors().forEach((key, value) -> result.getEncryptors().put(key, algorithmSwapper.swapToYamlConfiguration(value)));
        if (null != data.getEncryptMode()) {
            result.setEncryptMode(encryptModeSwapper.swapToYamlConfiguration(data.getEncryptMode()));
        }
        result.setQueryWithCipherColumn(data.isQueryWithCipherColumn());
        return result;
    }
    
    @Override
    public CompatibleEncryptRuleConfiguration swapToObject(final YamlCompatibleEncryptRuleConfiguration yamlConfig) {
        CompatibleEncryptRuleConfiguration result = new CompatibleEncryptRuleConfiguration(swapTables(yamlConfig), swapEncryptAlgorithm(yamlConfig), yamlConfig.isQueryWithCipherColumn());
        if (null != yamlConfig.getEncryptMode()) {
            result.setEncryptMode(encryptModeSwapper.swapToObject(yamlConfig.getEncryptMode()));
        }
        return result;
    }
    
    private Collection<CompatibleEncryptTableRuleConfiguration> swapTables(final YamlCompatibleEncryptRuleConfiguration yamlConfig) {
        Collection<CompatibleEncryptTableRuleConfiguration> result = new LinkedList<>();
        for (Entry<String, YamlCompatibleEncryptTableRuleConfiguration> entry : yamlConfig.getTables().entrySet()) {
            YamlCompatibleEncryptTableRuleConfiguration yamlCompatibleEncryptTableRuleConfig = entry.getValue();
            yamlCompatibleEncryptTableRuleConfig.setName(entry.getKey());
            result.add(tableSwapper.swapToObject(yamlCompatibleEncryptTableRuleConfig));
        }
        return result;
    }
    
    private Map<String, AlgorithmConfiguration> swapEncryptAlgorithm(final YamlCompatibleEncryptRuleConfiguration yamlConfig) {
        Map<String, AlgorithmConfiguration> result = new LinkedHashMap<>(yamlConfig.getEncryptors().size(), 1);
        for (Entry<String, YamlAlgorithmConfiguration> entry : yamlConfig.getEncryptors().entrySet()) {
            result.put(entry.getKey(), algorithmSwapper.swapToObject(entry.getValue()));
        }
        return result;
    }
    
    @Override
    public Class<CompatibleEncryptRuleConfiguration> getTypeClass() {
        return CompatibleEncryptRuleConfiguration.class;
    }
    
    @Override
    public String getRuleTagName() {
        return "ENCRYPT";
    }
    
    @Override
    public int getOrder() {
        return EncryptOrder.COMPATIBLE_ORDER;
    }
}
