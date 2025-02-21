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

import com.sphereex.dbplusengine.encrypt.config.rule.PlainColumnItemRuleConfiguration;
import com.sphereex.dbplusengine.encrypt.yaml.config.rule.YamlPlainColumnItemRuleConfiguration;
import org.apache.shardingsphere.infra.util.yaml.swapper.YamlConfigurationSwapper;

/**
 * YAML plain column item rule configuration swapper.
 */
public final class YamlPlainColumnItemRuleConfigurationSwapper implements YamlConfigurationSwapper<YamlPlainColumnItemRuleConfiguration, PlainColumnItemRuleConfiguration> {
    
    @Override
    public YamlPlainColumnItemRuleConfiguration swapToYamlConfiguration(final PlainColumnItemRuleConfiguration data) {
        YamlPlainColumnItemRuleConfiguration result = new YamlPlainColumnItemRuleConfiguration();
        result.setName(data.getName());
        result.setQueryWithPlain(data.isQueryWithPlain());
        return result;
    }
    
    @Override
    public PlainColumnItemRuleConfiguration swapToObject(final YamlPlainColumnItemRuleConfiguration yamlConfig) {
        PlainColumnItemRuleConfiguration result = new PlainColumnItemRuleConfiguration(yamlConfig.getName());
        result.setQueryWithPlain(yamlConfig.isQueryWithPlain());
        return result;
    }
}
