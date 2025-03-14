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

package com.sphereex.dbplusengine.encrypt.rule.builder;

import com.sphereex.dbplusengine.SphereEx;
import com.sphereex.dbplusengine.encrypt.config.rule.compatible.CompatibleEncryptRuleConfiguration;
import org.apache.shardingsphere.encrypt.constant.EncryptOrder;
import org.apache.shardingsphere.encrypt.rule.EncryptRule;
import org.apache.shardingsphere.infra.config.props.ConfigurationProperties;
import org.apache.shardingsphere.infra.database.core.type.DatabaseType;
import org.apache.shardingsphere.infra.instance.ComputeNodeInstanceContext;
import org.apache.shardingsphere.infra.metadata.database.resource.ResourceMetaData;
import org.apache.shardingsphere.infra.rule.ShardingSphereRule;
import org.apache.shardingsphere.infra.rule.builder.database.DatabaseRuleBuilder;

import java.util.Collection;

/**
 * Compatible encrypt rule builder.
 */
public final class CompatibleEncryptRuleBuilder implements DatabaseRuleBuilder<CompatibleEncryptRuleConfiguration> {
    
    @Override
    public EncryptRule build(final CompatibleEncryptRuleConfiguration ruleConfig, final String databaseName, final DatabaseType protocolType,
                             final ResourceMetaData resourceMetaData, final Collection<ShardingSphereRule> builtRules, final ComputeNodeInstanceContext computeNodeInstanceContext,
                             @SphereEx final ConfigurationProperties props) {
        return new EncryptRule(databaseName, ruleConfig.toEncryptRuleConfig(), resourceMetaData.getDataSourceMap(), builtRules);
    }
    
    @Override
    public int getOrder() {
        return EncryptOrder.COMPATIBLE_ORDER;
    }
    
    @Override
    public Class<CompatibleEncryptRuleConfiguration> getTypeClass() {
        return CompatibleEncryptRuleConfiguration.class;
    }
}
