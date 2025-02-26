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
import org.apache.shardingsphere.broadcast.metadata.nodepath.BroadcastRuleNodePathProvider;
import org.apache.shardingsphere.broadcast.rule.BroadcastRule;
import org.apache.shardingsphere.infra.algorithm.core.config.AlgorithmConfiguration;
import org.apache.shardingsphere.infra.algorithm.core.processor.AlgorithmChangedProcessor;

import java.util.LinkedList;
import java.util.Map;

/**
 * Broadcast key generators changed processor.
 */
public final class BroadcastKeyGeneratorsChangedProcessor extends AlgorithmChangedProcessor<BroadcastRuleConfiguration> {
    
    public BroadcastKeyGeneratorsChangedProcessor() {
        super(BroadcastRule.class);
    }
    
    @Override
    protected BroadcastRuleConfiguration createEmptyRuleConfiguration() {
        return new BroadcastRuleConfiguration(new LinkedList<>());
    }
    
    @Override
    protected Map<String, AlgorithmConfiguration> getAlgorithmConfigurations(final BroadcastRuleConfiguration currentRuleConfig) {
        return currentRuleConfig.getKeyGenerators();
    }
    
    @Override
    public Object getType() {
        return BroadcastRuleNodePathProvider.RULE_TYPE + "." + BroadcastRuleNodePathProvider.KEY_GENERATORS;
    }
}
