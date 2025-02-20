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

package com.sphereex.dbplusengine.encrypt.rule.changed;

import com.sphereex.dbplusengine.encrypt.config.rule.mode.EncryptModeRuleConfiguration;
import com.sphereex.dbplusengine.encrypt.yaml.config.rule.YamlEncryptModeRuleConfiguration;
import com.sphereex.dbplusengine.encrypt.yaml.swapper.rule.YamlEncryptModeRuleConfigurationSwapper;
import org.apache.shardingsphere.encrypt.config.EncryptRuleConfiguration;
import org.apache.shardingsphere.encrypt.metadata.nodepath.EncryptRuleNodePathProvider;
import org.apache.shardingsphere.encrypt.rule.EncryptRule;
import org.apache.shardingsphere.infra.metadata.database.ShardingSphereDatabase;
import org.apache.shardingsphere.infra.util.yaml.YamlEngine;
import org.apache.shardingsphere.mode.spi.rule.RuleItemConfigurationChangedProcessor;
import org.apache.shardingsphere.mode.spi.rule.item.alter.AlterRuleItem;
import org.apache.shardingsphere.mode.spi.rule.item.drop.DropRuleItem;

import java.util.LinkedHashMap;
import java.util.LinkedList;

/**
 * Encrypt mode changed processor.
 */
public final class EncryptModeChangedProcessor implements RuleItemConfigurationChangedProcessor<EncryptRuleConfiguration, EncryptModeRuleConfiguration> {
    
    @Override
    public EncryptModeRuleConfiguration swapRuleItemConfiguration(final AlterRuleItem alterRuleItem, final String yamlContent) {
        return new YamlEncryptModeRuleConfigurationSwapper().swapToObject(YamlEngine.unmarshal(yamlContent, YamlEncryptModeRuleConfiguration.class));
        
    }
    
    @Override
    public EncryptRuleConfiguration findRuleConfiguration(final ShardingSphereDatabase database) {
        return database.getRuleMetaData().findSingleRule(EncryptRule.class).map(EncryptRule::getConfiguration)
                .orElseGet(() -> new EncryptRuleConfiguration(new LinkedList<>(), new LinkedHashMap<>()));
    }
    
    @Override
    public void changeRuleItemConfiguration(final AlterRuleItem alterRuleItem, final EncryptRuleConfiguration currentRuleConfig, final EncryptModeRuleConfiguration toBeChangedItemConfig) {
        currentRuleConfig.setEncryptMode(toBeChangedItemConfig);
    }
    
    @Override
    public void dropRuleItemConfiguration(final DropRuleItem dropRuleItem, final EncryptRuleConfiguration currentRuleConfig) {
    }
    
    @Override
    public String getType() {
        return EncryptRuleNodePathProvider.RULE_TYPE + "." + EncryptRuleNodePathProvider.ENCRYPT_MODE;
    }
}
