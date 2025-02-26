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
import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

class YamlBroadcastKeyGenerateStrategyConfigurationSwapperTest {
    
    @Test
    void assertSwapToYamlConfiguration() {
        YamlBroadcastKeyGenerateStrategyConfigurationSwapper swapper = new YamlBroadcastKeyGenerateStrategyConfigurationSwapper();
        YamlBroadcastKeyGenerateStrategyConfiguration actual = swapper.swapToYamlConfiguration(new BroadcastKeyGenerateStrategyConfiguration("t_config", "id", "snowflake"));
        assertThat(actual.getLogicTable(), is("t_config"));
        assertThat(actual.getKeyGenerateColumn(), is("id"));
        assertThat(actual.getKeyGeneratorName(), is("snowflake"));
    }
    
    @Test
    void assertSwapToObject() {
        YamlBroadcastKeyGenerateStrategyConfigurationSwapper swapper = new YamlBroadcastKeyGenerateStrategyConfigurationSwapper();
        YamlBroadcastKeyGenerateStrategyConfiguration keyGenerateStrategyConfig = new YamlBroadcastKeyGenerateStrategyConfiguration();
        keyGenerateStrategyConfig.setLogicTable("t_config");
        keyGenerateStrategyConfig.setKeyGenerateColumn("id");
        keyGenerateStrategyConfig.setKeyGeneratorName("snowflake");
        BroadcastKeyGenerateStrategyConfiguration actual = swapper.swapToObject(keyGenerateStrategyConfig);
        assertThat(actual.getLogicTable(), is("t_config"));
        assertThat(actual.getKeyGenerateColumn(), is("id"));
        assertThat(actual.getKeyGeneratorName(), is("snowflake"));
    }
}
