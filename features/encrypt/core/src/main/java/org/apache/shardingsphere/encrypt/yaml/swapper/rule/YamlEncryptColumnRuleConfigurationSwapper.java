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

package org.apache.shardingsphere.encrypt.yaml.swapper.rule;

import com.sphereex.dbplusengine.SphereEx;
import com.sphereex.dbplusengine.encrypt.yaml.swapper.rule.YamlPlainColumnItemRuleConfigurationSwapper;
import org.apache.shardingsphere.encrypt.config.rule.EncryptColumnRuleConfiguration;
import org.apache.shardingsphere.encrypt.yaml.config.rule.YamlEncryptColumnRuleConfiguration;
import org.apache.shardingsphere.infra.util.yaml.swapper.YamlConfigurationSwapper;

/**
 * YAML encrypt column rule configuration swapper.
 */
public final class YamlEncryptColumnRuleConfigurationSwapper implements YamlConfigurationSwapper<YamlEncryptColumnRuleConfiguration, EncryptColumnRuleConfiguration> {
    
    private final YamlEncryptColumnItemRuleConfigurationSwapper encryptColumnItemSwapper = new YamlEncryptColumnItemRuleConfigurationSwapper();
    
    @SphereEx
    private final YamlPlainColumnItemRuleConfigurationSwapper plainColumnItemSwapper = new YamlPlainColumnItemRuleConfigurationSwapper();
    
    @Override
    public YamlEncryptColumnRuleConfiguration swapToYamlConfiguration(final EncryptColumnRuleConfiguration data) {
        YamlEncryptColumnRuleConfiguration result = new YamlEncryptColumnRuleConfiguration();
        result.setName(data.getName());
        // SPEX ADDED: BEGIN
        data.getDataType().ifPresent(result::setDataType);
        data.getPlain().ifPresent(optional -> result.setPlain(plainColumnItemSwapper.swapToYamlConfiguration(optional)));
        data.getOrderQuery().ifPresent(optional -> result.setOrderQuery(encryptColumnItemSwapper.swapToYamlConfiguration(optional)));
        // SPEX ADDED: END
        result.setCipher(encryptColumnItemSwapper.swapToYamlConfiguration(data.getCipher()));
        data.getLikeQuery().ifPresent(optional -> result.setLikeQuery(encryptColumnItemSwapper.swapToYamlConfiguration(optional)));
        data.getAssistedQuery().ifPresent(optional -> result.setAssistedQuery(encryptColumnItemSwapper.swapToYamlConfiguration(optional)));
        return result;
    }
    
    @Override
    public EncryptColumnRuleConfiguration swapToObject(final YamlEncryptColumnRuleConfiguration yamlConfig) {
        EncryptColumnRuleConfiguration result = new EncryptColumnRuleConfiguration(yamlConfig.getName(), encryptColumnItemSwapper.swapToObject(yamlConfig.getCipher()));
        if (null != yamlConfig.getAssistedQuery()) {
            result.setAssistedQuery(encryptColumnItemSwapper.swapToObject(yamlConfig.getAssistedQuery()));
        }
        if (null != yamlConfig.getLikeQuery()) {
            result.setLikeQuery(encryptColumnItemSwapper.swapToObject(yamlConfig.getLikeQuery()));
        }
        // SPEX ADDED: BEGIN
        if (null != yamlConfig.getOrderQuery()) {
            result.setOrderQuery(encryptColumnItemSwapper.swapToObject(yamlConfig.getOrderQuery()));
        }
        if (null != yamlConfig.getPlain()) {
            result.setPlain(plainColumnItemSwapper.swapToObject(yamlConfig.getPlain()));
        }
        result.setDataType(yamlConfig.getDataType());
        // SPEX ADDED: END
        return result;
    }
}
