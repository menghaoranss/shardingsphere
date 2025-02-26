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

package org.apache.shardingsphere.broadcast.yaml.swapper;

import com.sphereex.dbplusengine.SphereEx;
import com.sphereex.dbplusengine.SphereEx.Type;
import com.sphereex.dbplusengine.broadcast.yaml.config.keygen.YamlBroadcastKeyGenerateStrategyConfiguration;
import com.sphereex.dbplusengine.broadcast.yaml.swapper.keygen.YamlBroadcastKeyGenerateStrategyConfigurationSwapper;
import org.apache.shardingsphere.broadcast.config.BroadcastRuleConfiguration;
import org.apache.shardingsphere.broadcast.constant.BroadcastOrder;
import org.apache.shardingsphere.broadcast.yaml.config.YamlBroadcastRuleConfiguration;
import org.apache.shardingsphere.infra.algorithm.core.yaml.YamlAlgorithmConfigurationSwapper;
import org.apache.shardingsphere.infra.yaml.config.swapper.rule.YamlRuleConfigurationSwapper;

import java.util.Map.Entry;

/**
 * YAML broadcast rule configuration swapper.
 */
public final class YamlBroadcastRuleConfigurationSwapper implements YamlRuleConfigurationSwapper<YamlBroadcastRuleConfiguration, BroadcastRuleConfiguration> {
    
    @SphereEx
    private final YamlBroadcastKeyGenerateStrategyConfigurationSwapper keyGenerateStrategySwapper = new YamlBroadcastKeyGenerateStrategyConfigurationSwapper();
    
    @SphereEx
    private final YamlAlgorithmConfigurationSwapper algorithmSwapper = new YamlAlgorithmConfigurationSwapper();
    
    @Override
    public YamlBroadcastRuleConfiguration swapToYamlConfiguration(final BroadcastRuleConfiguration data) {
        YamlBroadcastRuleConfiguration result = new YamlBroadcastRuleConfiguration();
        data.getTables().forEach(each -> result.getTables().add(each));
        // SPEX ADDED: BEGIN
        data.getActualDataSourceNames().forEach(each -> result.getActualDataSourceNames().add(each));
        data.getKeyGenerateStrategies().forEach((key, value) -> result.getKeyGenerateStrategies().put(key, keyGenerateStrategySwapper.swapToYamlConfiguration(value)));
        data.getKeyGenerators().forEach((key, value) -> result.getKeyGenerators().put(key, algorithmSwapper.swapToYamlConfiguration(value)));
        // SPEX ADDED: END
        return result;
    }
    
    @SphereEx(Type.MODIFY)
    @Override
    public BroadcastRuleConfiguration swapToObject(final YamlBroadcastRuleConfiguration yamlConfig) {
        BroadcastRuleConfiguration result = new BroadcastRuleConfiguration(yamlConfig.getTables(), yamlConfig.getActualDataSourceNames());
        for (Entry<String, YamlBroadcastKeyGenerateStrategyConfiguration> entry : yamlConfig.getKeyGenerateStrategies().entrySet()) {
            YamlBroadcastKeyGenerateStrategyConfiguration keyGenerateStrategyConfig = entry.getValue();
            keyGenerateStrategyConfig.setLogicTable(entry.getKey());
            result.getKeyGenerateStrategies().put(entry.getKey(), keyGenerateStrategySwapper.swapToObject(keyGenerateStrategyConfig));
        }
        yamlConfig.getKeyGenerators().forEach((key, value) -> result.getKeyGenerators().put(key, algorithmSwapper.swapToObject(value)));
        return result;
    }
    
    @Override
    public Class<BroadcastRuleConfiguration> getTypeClass() {
        return BroadcastRuleConfiguration.class;
    }
    
    @Override
    public int getOrder() {
        return BroadcastOrder.ORDER;
    }
    
    @Override
    public String getRuleTagName() {
        return "BROADCAST";
    }
}
