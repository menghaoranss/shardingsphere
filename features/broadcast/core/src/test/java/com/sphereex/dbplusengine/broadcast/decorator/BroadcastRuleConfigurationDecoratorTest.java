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

package com.sphereex.dbplusengine.broadcast.decorator;

import com.sphereex.dbplusengine.SphereEx;
import com.sphereex.dbplusengine.SphereEx.Type;
import com.sphereex.dbplusengine.broadcast.config.keygen.BroadcastKeyGenerateStrategyConfiguration;
import org.apache.shardingsphere.broadcast.config.BroadcastRuleConfiguration;
import org.apache.shardingsphere.broadcast.rule.BroadcastRule;
import org.apache.shardingsphere.infra.algorithm.core.config.AlgorithmConfiguration;
import org.apache.shardingsphere.infra.config.props.ConfigurationProperties;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class BroadcastRuleConfigurationDecoratorTest {
    
    @Test
    void assertDecorateWhenActualDataSourceNamesAreEmpty() {
        BroadcastRuleConfigurationDecorator decorator = new BroadcastRuleConfigurationDecorator();
        BroadcastRule broadcastRule = mock(BroadcastRule.class);
        when(broadcastRule.getActualDataSourceNames()).thenReturn(Arrays.asList("ds_0", "ds_1"));
        @SphereEx(Type.MODIFY)
        BroadcastRuleConfiguration actual = decorator.decorate("foo_db", Collections.emptyMap(), Collections.singleton(broadcastRule), createRuleConfiguration(), mock(ConfigurationProperties.class));
        assertThat(actual.getTables(), is(Collections.singleton("t_config")));
        assertThat(actual.getActualDataSourceNames(), is(new LinkedHashSet<>(Arrays.asList("ds_0", "ds_1"))));
        assertThat(actual.getKeyGenerateStrategies().size(), is(1));
        BroadcastKeyGenerateStrategyConfiguration strategyConfig = actual.getKeyGenerateStrategies().get("t_config");
        assertThat(strategyConfig.getLogicTable(), is("t_config"));
        assertThat(strategyConfig.getKeyGenerateColumn(), is("id"));
        assertThat(strategyConfig.getKeyGeneratorName(), is("snowflake"));
        assertThat(actual.getKeyGenerators().size(), is(1));
        assertTrue(actual.getKeyGenerators().containsKey("snowflake"));
    }
    
    @Test
    void assertDecorateWhenActualDataSourceNamesAreNotEmpty() {
        BroadcastRuleConfigurationDecorator decorator = new BroadcastRuleConfigurationDecorator();
        BroadcastRuleConfiguration ruleConfig = createRuleConfiguration();
        ruleConfig.getActualDataSourceNames().addAll(Arrays.asList("ds_0", "ds_1"));
        @SphereEx(Type.MODIFY)
        BroadcastRuleConfiguration actual = decorator.decorate("foo_db", Collections.emptyMap(), Collections.singleton(mock(BroadcastRule.class)), ruleConfig, mock(ConfigurationProperties.class));
        assertThat(actual.getTables(), is(Collections.singleton("t_config")));
        assertThat(actual.getActualDataSourceNames(), is(new LinkedHashSet<>(Arrays.asList("ds_0", "ds_1"))));
        assertThat(actual.getKeyGenerateStrategies().size(), is(1));
        BroadcastKeyGenerateStrategyConfiguration strategyConfig = actual.getKeyGenerateStrategies().get("t_config");
        assertThat(strategyConfig.getLogicTable(), is("t_config"));
        assertThat(strategyConfig.getKeyGenerateColumn(), is("id"));
        assertThat(strategyConfig.getKeyGeneratorName(), is("snowflake"));
        assertThat(actual.getKeyGenerators().size(), is(1));
        assertTrue(actual.getKeyGenerators().containsKey("snowflake"));
    }
    
    private BroadcastRuleConfiguration createRuleConfiguration() {
        BroadcastRuleConfiguration result = new BroadcastRuleConfiguration(Collections.singleton("t_config"));
        result.getKeyGenerateStrategies().put("t_config", new BroadcastKeyGenerateStrategyConfiguration("t_config", "id", "snowflake"));
        result.getKeyGenerators().put("snowflake", mock(AlgorithmConfiguration.class));
        return result;
    }
}
