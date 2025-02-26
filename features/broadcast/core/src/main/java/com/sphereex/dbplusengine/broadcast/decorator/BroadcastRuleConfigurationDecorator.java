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

import org.apache.shardingsphere.broadcast.config.BroadcastRuleConfiguration;
import org.apache.shardingsphere.broadcast.rule.BroadcastRule;
import org.apache.shardingsphere.infra.config.rule.decorator.RuleConfigurationDecorator;
import org.apache.shardingsphere.infra.rule.ShardingSphereRule;

import javax.sql.DataSource;
import java.util.Collection;
import java.util.Map;

/**
 * Broadcast rule configuration decorator.
 */
public final class BroadcastRuleConfigurationDecorator implements RuleConfigurationDecorator<BroadcastRuleConfiguration> {
    
    @Override
    public BroadcastRuleConfiguration decorate(final String databaseName, final Map<String, DataSource> dataSources,
                                               final Collection<ShardingSphereRule> builtRules, final BroadcastRuleConfiguration ruleConfig) {
        if (!ruleConfig.getActualDataSourceNames().isEmpty()) {
            return ruleConfig;
        }
        BroadcastRuleConfiguration result = new BroadcastRuleConfiguration(ruleConfig.getTables());
        builtRules.stream().filter(BroadcastRule.class::isInstance).findFirst().ifPresent(each -> result.getActualDataSourceNames().addAll(((BroadcastRule) each).getActualDataSourceNames()));
        result.getKeyGenerateStrategies().putAll(ruleConfig.getKeyGenerateStrategies());
        result.getKeyGenerators().putAll(ruleConfig.getKeyGenerators());
        return result;
    }
    
    @Override
    public Class<BroadcastRuleConfiguration> getType() {
        return BroadcastRuleConfiguration.class;
    }
}
