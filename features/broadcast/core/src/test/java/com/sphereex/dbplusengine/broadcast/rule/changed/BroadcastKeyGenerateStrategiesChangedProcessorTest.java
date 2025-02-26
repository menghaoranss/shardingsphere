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

package com.sphereex.dbplusengine.broadcast.rule.changed;

import com.sphereex.dbplusengine.broadcast.config.keygen.BroadcastKeyGenerateStrategyConfiguration;
import com.sphereex.dbplusengine.broadcast.yaml.config.keygen.YamlBroadcastKeyGenerateStrategyConfiguration;
import org.apache.shardingsphere.broadcast.config.BroadcastRuleConfiguration;
import org.apache.shardingsphere.broadcast.rule.BroadcastRule;
import org.apache.shardingsphere.infra.metadata.database.ShardingSphereDatabase;
import org.apache.shardingsphere.infra.metadata.database.rule.RuleMetaData;
import org.apache.shardingsphere.infra.spi.type.typed.TypedSPILoader;
import org.apache.shardingsphere.infra.util.yaml.YamlEngine;
import org.apache.shardingsphere.mode.spi.rule.RuleItemConfigurationChangedProcessor;
import org.apache.shardingsphere.mode.spi.rule.item.alter.AlterNamedRuleItem;
import org.apache.shardingsphere.mode.spi.rule.item.drop.DropNamedRuleItem;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.apache.shardingsphere.test.matcher.ShardingSphereAssertionMatchers.deepEqual;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class BroadcastKeyGenerateStrategiesChangedProcessorTest {
    
    @SuppressWarnings("unchecked")
    private final RuleItemConfigurationChangedProcessor<BroadcastRuleConfiguration, BroadcastKeyGenerateStrategyConfiguration> processor = TypedSPILoader.getService(
            RuleItemConfigurationChangedProcessor.class, "broadcast.key_generate_strategies");
    
    @Test
    void assertSwapRuleItemConfiguration() {
        AlterNamedRuleItem alterNamedRuleItem = mock(AlterNamedRuleItem.class);
        BroadcastKeyGenerateStrategyConfiguration actual = processor.swapRuleItemConfiguration(alterNamedRuleItem, createYAMLContent());
        assertThat(actual, deepEqual(new BroadcastKeyGenerateStrategyConfiguration("foo_tbl", "foo_col", "foo_algo")));
    }
    
    private String createYAMLContent() {
        YamlBroadcastKeyGenerateStrategyConfiguration yamlConfig = new YamlBroadcastKeyGenerateStrategyConfiguration();
        yamlConfig.setLogicTable("foo_tbl");
        yamlConfig.setKeyGenerateColumn("foo_col");
        yamlConfig.setKeyGeneratorName("foo_algo");
        return YamlEngine.marshal(yamlConfig);
    }
    
    @Test
    void assertFindRuleConfiguration() {
        BroadcastRuleConfiguration ruleConfig = mock(BroadcastRuleConfiguration.class);
        assertThat(processor.findRuleConfiguration(mockDatabase(ruleConfig)), is(ruleConfig));
    }
    
    private ShardingSphereDatabase mockDatabase(final BroadcastRuleConfiguration ruleConfig) {
        BroadcastRule rule = mock(BroadcastRule.class);
        when(rule.getConfiguration()).thenReturn(ruleConfig);
        ShardingSphereDatabase result = mock(ShardingSphereDatabase.class);
        when(result.getRuleMetaData()).thenReturn(new RuleMetaData(Collections.singleton(rule)));
        return result;
    }
    
    @Test
    void assertChangeRuleItemConfiguration() {
        AlterNamedRuleItem alterNamedRuleItem = mock(AlterNamedRuleItem.class);
        when(alterNamedRuleItem.getItemName()).thenReturn("foo_strategy");
        BroadcastRuleConfiguration currentRuleConfig = createCurrentRuleConfiguration();
        BroadcastKeyGenerateStrategyConfiguration toBeChangedItemConfig = new BroadcastKeyGenerateStrategyConfiguration("bar_tbl", "bar_col", "bar_algo");
        processor.changeRuleItemConfiguration(alterNamedRuleItem, currentRuleConfig, toBeChangedItemConfig);
        assertThat(currentRuleConfig.getKeyGenerateStrategies().size(), is(1));
        assertThat(currentRuleConfig.getKeyGenerateStrategies().get("foo_strategy"), deepEqual(new BroadcastKeyGenerateStrategyConfiguration("bar_tbl", "bar_col", "bar_algo")));
    }
    
    @Test
    void assertDropRuleItemConfiguration() {
        DropNamedRuleItem dropNamedRuleItem = mock(DropNamedRuleItem.class);
        when(dropNamedRuleItem.getItemName()).thenReturn("foo_strategy");
        BroadcastRuleConfiguration currentRuleConfig = createCurrentRuleConfiguration();
        processor.dropRuleItemConfiguration(dropNamedRuleItem, currentRuleConfig);
        assertTrue(currentRuleConfig.getKeyGenerateStrategies().isEmpty());
    }
    
    private BroadcastRuleConfiguration createCurrentRuleConfiguration() {
        BroadcastRuleConfiguration result = new BroadcastRuleConfiguration(Collections.emptyList());
        result.getKeyGenerateStrategies().put("foo_strategy", new BroadcastKeyGenerateStrategyConfiguration("foo_tbl", "foo_col", "foo_algo"));
        return result;
    }
}
