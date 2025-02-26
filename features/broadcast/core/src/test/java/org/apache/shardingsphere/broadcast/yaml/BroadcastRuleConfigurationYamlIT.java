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

package org.apache.shardingsphere.broadcast.yaml;

import com.sphereex.dbplusengine.SphereEx;
import com.sphereex.dbplusengine.SphereEx.Type;
import com.sphereex.dbplusengine.broadcast.config.keygen.BroadcastKeyGenerateStrategyConfiguration;
import org.apache.shardingsphere.broadcast.config.BroadcastRuleConfiguration;
import org.apache.shardingsphere.broadcast.yaml.config.YamlBroadcastRuleConfiguration;
import org.apache.shardingsphere.infra.algorithm.core.config.AlgorithmConfiguration;
import org.apache.shardingsphere.infra.yaml.config.pojo.rule.YamlRuleConfiguration;
import org.apache.shardingsphere.test.it.yaml.YamlRuleConfigurationIT;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Properties;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

class BroadcastRuleConfigurationYamlIT extends YamlRuleConfigurationIT {
    
    @SphereEx(Type.MODIFY)
    BroadcastRuleConfigurationYamlIT() {
        super("yaml/broadcast-rule.yaml", getExpectedRuleConfiguration());
    }
    
    @SphereEx
    private static BroadcastRuleConfiguration getExpectedRuleConfiguration() {
        BroadcastRuleConfiguration result = new BroadcastRuleConfiguration(Arrays.asList("foo_tbl", "bar_tbl"), Arrays.asList("ds_0", "ds_1"));
        result.getKeyGenerateStrategies().put("foo_tbl", new BroadcastKeyGenerateStrategyConfiguration("foo_tbl", "id", "fixture"));
        result.getKeyGenerators().put("fixture", new AlgorithmConfiguration("FIXTURE", new Properties()));
        return result;
    }
    
    @SphereEx
    @Override
    protected boolean assertYamlConfiguration(final YamlRuleConfiguration actual) {
        assertBroadcastRule((YamlBroadcastRuleConfiguration) actual);
        return true;
    }
    
    @SphereEx
    private void assertBroadcastRule(final YamlBroadcastRuleConfiguration actual) {
        assertThat(actual.getTables().size(), is(2));
        assertThat(new ArrayList<>(actual.getTables()).get(0), is("foo_tbl"));
        assertThat(new ArrayList<>(actual.getTables()).get(1), is("bar_tbl"));
        assertThat(actual.getActualDataSourceNames().size(), is(2));
        assertThat(new ArrayList<>(actual.getActualDataSourceNames()).get(0), is("ds_0"));
        assertThat(new ArrayList<>(actual.getActualDataSourceNames()).get(1), is("ds_1"));
        assertThat(actual.getKeyGenerateStrategies().size(), is(1));
        assertThat(actual.getKeyGenerateStrategies().get("foo_tbl").getKeyGenerateColumn(), is("id"));
        assertThat(actual.getKeyGenerateStrategies().get("foo_tbl").getKeyGeneratorName(), is("fixture"));
        assertThat(actual.getKeyGenerators().size(), is(1));
        assertThat(actual.getKeyGenerators().get("fixture").getType(), is("FIXTURE"));
    }
}
