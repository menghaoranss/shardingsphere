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

import org.apache.shardingsphere.broadcast.config.BroadcastRuleConfiguration;
import org.apache.shardingsphere.broadcast.rule.BroadcastRule;
import org.apache.shardingsphere.infra.metadata.database.ShardingSphereDatabase;
import org.apache.shardingsphere.infra.metadata.database.rule.RuleMetaData;
import org.apache.shardingsphere.infra.spi.type.typed.TypedSPILoader;
import org.apache.shardingsphere.mode.spi.rule.RuleItemConfigurationChangedProcessor;
import org.apache.shardingsphere.mode.spi.rule.item.alter.AlterNamedRuleItem;
import org.apache.shardingsphere.mode.spi.rule.item.drop.DropNamedRuleItem;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.LinkedList;

import static org.apache.shardingsphere.test.matcher.ShardingSphereAssertionMatchers.deepEqual;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class BroadcastActualDataSourceNamesChangedProcessorTest {
    
    @SuppressWarnings("unchecked")
    private final RuleItemConfigurationChangedProcessor<BroadcastRuleConfiguration, BroadcastRuleConfiguration> processor = TypedSPILoader.getService(
            RuleItemConfigurationChangedProcessor.class, "broadcast.actual_data_source_names");
    
    @Test
    void assertSwapRuleItemConfiguration() {
        AlterNamedRuleItem alterNamedRuleItem = mock(AlterNamedRuleItem.class);
        BroadcastRuleConfiguration actual = processor.swapRuleItemConfiguration(alterNamedRuleItem, "- foo_ds");
        assertThat(actual, deepEqual(new BroadcastRuleConfiguration(new LinkedList<>(), Collections.singleton("foo_ds"))));
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
        BroadcastRuleConfiguration toBeChangedItemConfig = new BroadcastRuleConfiguration(Collections.emptyList(), Collections.singletonList("bar_ds"));
        processor.changeRuleItemConfiguration(alterNamedRuleItem, currentRuleConfig, toBeChangedItemConfig);
        assertThat(currentRuleConfig.getActualDataSourceNames().size(), is(1));
        assertThat(currentRuleConfig.getActualDataSourceNames(), is(Collections.singletonList("bar_ds")));
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
        return new BroadcastRuleConfiguration(Collections.emptyList(), new LinkedList<>(Collections.singleton("foo_ds")));
    }
}
