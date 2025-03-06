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

package org.apache.shardingsphere.single.rule.builder;

import com.sphereex.dbplusengine.SphereEx;
import com.sphereex.dbplusengine.SphereEx.Type;
import org.apache.shardingsphere.infra.config.props.ConfigurationProperties;
import org.apache.shardingsphere.infra.config.props.ConfigurationPropertyKey;
import org.apache.shardingsphere.infra.database.mysql.type.MySQLDatabaseType;
import org.apache.shardingsphere.infra.instance.ComputeNodeInstanceContext;
import org.apache.shardingsphere.infra.metadata.database.resource.ResourceMetaData;
import org.apache.shardingsphere.infra.rule.ShardingSphereRule;
import org.apache.shardingsphere.infra.rule.builder.database.DatabaseRuleBuilder;
import org.apache.shardingsphere.infra.rule.scope.DatabaseRule;
import org.apache.shardingsphere.infra.spi.type.ordered.OrderedSPILoader;
import org.apache.shardingsphere.single.config.SingleRuleConfiguration;
import org.apache.shardingsphere.single.constant.SingleTableConstants;
import org.apache.shardingsphere.single.rule.SingleRule;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class SingleRuleBuilderTest {
    
    @SuppressWarnings({"rawtypes", "unchecked"})
    @Test
    void assertBuild() {
        DatabaseRuleBuilder builder = OrderedSPILoader.getServices(DatabaseRuleBuilder.class).iterator().next();
        SingleRuleConfiguration ruleConfig = mock(SingleRuleConfiguration.class);
        when(ruleConfig.getTables()).thenReturn(Collections.singleton(SingleTableConstants.ALL_TABLES));
        @SphereEx(Type.MODIFY)
        DatabaseRule actual = builder.build(ruleConfig, "",
                new MySQLDatabaseType(), mock(ResourceMetaData.class), Collections.singleton(mock(ShardingSphereRule.class, RETURNS_DEEP_STUBS)), mock(ComputeNodeInstanceContext.class),
                mockProperties());
        assertThat(actual, instanceOf(SingleRule.class));
    }
    
    @SuppressWarnings({"rawtypes", "unchecked"})
    @Test
    void assertBuildWithDefaultDataSource() {
        DatabaseRuleBuilder builder = OrderedSPILoader.getServices(DatabaseRuleBuilder.class).iterator().next();
        @SphereEx(Type.MODIFY)
        DatabaseRule actual = builder.build(new SingleRuleConfiguration(Collections.singleton(SingleTableConstants.ALL_TABLES), "foo_ds"), "", new MySQLDatabaseType(), mock(ResourceMetaData.class),
                Collections.singleton(mock(ShardingSphereRule.class, RETURNS_DEEP_STUBS)), mock(ComputeNodeInstanceContext.class), mockProperties());
        assertThat(actual, instanceOf(SingleRule.class));
    }
    
    @SphereEx
    private ConfigurationProperties mockProperties() {
        ConfigurationProperties result = mock(ConfigurationProperties.class);
        when(result.getValue(ConfigurationPropertyKey.LOAD_METADATA_IGNORE_TABLES)).thenReturn("");
        return result;
    }
}
