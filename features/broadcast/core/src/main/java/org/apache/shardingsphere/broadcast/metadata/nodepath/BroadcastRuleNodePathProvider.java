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

package org.apache.shardingsphere.broadcast.metadata.nodepath;

import com.sphereex.dbplusengine.SphereEx;
import com.sphereex.dbplusengine.SphereEx.Type;
import org.apache.shardingsphere.broadcast.config.BroadcastRuleConfiguration;
import org.apache.shardingsphere.infra.config.rule.RuleConfiguration;
import org.apache.shardingsphere.mode.node.path.rule.RuleNodePath;
import org.apache.shardingsphere.mode.node.spi.RuleNodePathProvider;

import java.util.Arrays;

/**
 * Broadcast rule node path provider.
 */
public final class BroadcastRuleNodePathProvider implements RuleNodePathProvider {
    
    public static final String RULE_TYPE = "broadcast";
    
    public static final String TABLES = "tables";
    
    @SphereEx
    public static final String ACTUAL_DATA_SOURCE_NAMES = "actual_data_source_names";
    
    @SphereEx
    public static final String KEY_GENERATE_STRATEGIES = "key_generate_strategies";
    
    @SphereEx
    public static final String KEY_GENERATORS = "key_generators";
    
    @SphereEx(Type.MODIFY)
    private static final RuleNodePath INSTANCE = new RuleNodePath(RULE_TYPE, Arrays.asList(KEY_GENERATE_STRATEGIES, KEY_GENERATORS),
            Arrays.asList(TABLES, ACTUAL_DATA_SOURCE_NAMES));
    
    @Override
    public RuleNodePath getRuleNodePath() {
        return INSTANCE;
    }
    
    @Override
    public Class<? extends RuleConfiguration> getType() {
        return BroadcastRuleConfiguration.class;
    }
}
