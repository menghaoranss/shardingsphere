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

package org.apache.shardingsphere.broadcast.config;

import com.sphereex.dbplusengine.SphereEx;
import com.sphereex.dbplusengine.broadcast.config.keygen.BroadcastKeyGenerateStrategyConfiguration;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.apache.shardingsphere.infra.algorithm.core.config.AlgorithmConfiguration;
import org.apache.shardingsphere.infra.config.rule.function.DistributedRuleConfiguration;
import org.apache.shardingsphere.infra.config.rule.scope.DatabaseRuleConfiguration;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;

/**
 * Broadcast rule configuration.
 */
@RequiredArgsConstructor
@Getter
public final class BroadcastRuleConfiguration implements DatabaseRuleConfiguration, DistributedRuleConfiguration {
    
    private final Collection<String> tables;
    
    @SphereEx
    private final Collection<String> actualDataSourceNames;
    
    @SphereEx
    private final Map<String, BroadcastKeyGenerateStrategyConfiguration> keyGenerateStrategies = new LinkedHashMap<>();
    
    @SphereEx
    private final Map<String, AlgorithmConfiguration> keyGenerators = new LinkedHashMap<>();
    
    @SphereEx
    public BroadcastRuleConfiguration(final Collection<String> tables) {
        this(tables, new LinkedHashSet<>());
    }
}
