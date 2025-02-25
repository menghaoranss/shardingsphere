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

import com.sphereex.dbplusengine.encrypt.config.rule.compatible.CompatibleEncryptColumnRuleConfiguration;
import com.sphereex.dbplusengine.encrypt.config.rule.compatible.CompatibleEncryptTableRuleConfiguration;
import com.sphereex.dbplusengine.encrypt.yaml.config.rule.YamlCompatibleEncryptColumnRuleConfiguration;
import com.sphereex.dbplusengine.encrypt.yaml.config.rule.YamlCompatibleEncryptTableRuleConfiguration;
import org.apache.shardingsphere.infra.util.yaml.swapper.YamlConfigurationSwapper;

import java.util.Collection;
import java.util.LinkedList;
import java.util.Map.Entry;

/**
 * YAML compatible encrypt table configuration swapper.
 */
public final class YamlCompatibleEncryptTableRuleConfigurationSwapper implements YamlConfigurationSwapper<YamlCompatibleEncryptTableRuleConfiguration, CompatibleEncryptTableRuleConfiguration> {
    
    private final YamlCompatibleEncryptColumnRuleConfigurationSwapper columnSwapper = new YamlCompatibleEncryptColumnRuleConfigurationSwapper();
    
    @Override
    public YamlCompatibleEncryptTableRuleConfiguration swapToYamlConfiguration(final CompatibleEncryptTableRuleConfiguration data) {
        YamlCompatibleEncryptTableRuleConfiguration result = new YamlCompatibleEncryptTableRuleConfiguration();
        for (CompatibleEncryptColumnRuleConfiguration each : data.getColumns()) {
            result.getColumns().put(each.getLogicColumn(), columnSwapper.swapToYamlConfiguration(each));
        }
        result.setName(data.getName());
        result.setQueryWithCipherColumn(data.getQueryWithCipherColumn());
        return result;
    }
    
    @Override
    public CompatibleEncryptTableRuleConfiguration swapToObject(final YamlCompatibleEncryptTableRuleConfiguration yamlConfig) {
        Collection<CompatibleEncryptColumnRuleConfiguration> columns = new LinkedList<>();
        for (Entry<String, YamlCompatibleEncryptColumnRuleConfiguration> entry : yamlConfig.getColumns().entrySet()) {
            YamlCompatibleEncryptColumnRuleConfiguration yamlEncryptColumnRuleConfig = entry.getValue();
            yamlEncryptColumnRuleConfig.setLogicColumn(entry.getKey());
            columns.add(columnSwapper.swapToObject(yamlEncryptColumnRuleConfig));
        }
        return new CompatibleEncryptTableRuleConfiguration(yamlConfig.getName(), columns, yamlConfig.getQueryWithCipherColumn());
    }
}
