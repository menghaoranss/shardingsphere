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

package com.sphereex.dbplusengine.broadcast.yaml.swapper.keygen;

import com.sphereex.dbplusengine.broadcast.config.keygen.BroadcastKeyGenerateStrategyConfiguration;
import com.sphereex.dbplusengine.broadcast.yaml.config.keygen.YamlBroadcastKeyGenerateStrategyConfiguration;
import org.apache.shardingsphere.infra.util.yaml.swapper.YamlConfigurationSwapper;

/**
 * YAML broadcast key generate strategy configuration swapper.
 */
public final class YamlBroadcastKeyGenerateStrategyConfigurationSwapper implements YamlConfigurationSwapper<YamlBroadcastKeyGenerateStrategyConfiguration, BroadcastKeyGenerateStrategyConfiguration> {
    
    @Override
    public YamlBroadcastKeyGenerateStrategyConfiguration swapToYamlConfiguration(final BroadcastKeyGenerateStrategyConfiguration data) {
        YamlBroadcastKeyGenerateStrategyConfiguration result = new YamlBroadcastKeyGenerateStrategyConfiguration();
        result.setLogicTable(data.getLogicTable());
        result.setKeyGenerateColumn(data.getKeyGenerateColumn());
        result.setKeyGeneratorName(data.getKeyGeneratorName());
        return result;
    }
    
    @Override
    public BroadcastKeyGenerateStrategyConfiguration swapToObject(final YamlBroadcastKeyGenerateStrategyConfiguration yamlConfig) {
        return new BroadcastKeyGenerateStrategyConfiguration(yamlConfig.getLogicTable(), yamlConfig.getKeyGenerateColumn(), yamlConfig.getKeyGeneratorName());
    }
}
