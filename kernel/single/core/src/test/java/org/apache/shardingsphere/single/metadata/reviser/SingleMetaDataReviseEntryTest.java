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

package org.apache.shardingsphere.single.metadata.reviser;

import com.sphereex.dbplusengine.SphereEx;
import com.sphereex.dbplusengine.SphereEx.Type;
import org.apache.shardingsphere.infra.config.props.ConfigurationProperties;
import org.apache.shardingsphere.infra.config.props.ConfigurationPropertyKey;
import org.apache.shardingsphere.single.config.SingleRuleConfiguration;
import org.apache.shardingsphere.single.metadata.reviser.constraint.SingleConstraintReviser;
import org.apache.shardingsphere.single.rule.SingleRule;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Optional;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@Disabled
class SingleMetaDataReviseEntryTest {
    
    private final SingleMetaDataReviseEntry reviseEntry = new SingleMetaDataReviseEntry();
    
    @Test
    void assertGetConstraintReviser() {
        SingleRuleConfiguration ruleConfig = new SingleRuleConfiguration();
        // SPEX ADDED: BEGIN
        ConfigurationProperties props = mock(ConfigurationProperties.class);
        when(props.getValue(ConfigurationPropertyKey.LOAD_METADATA_IGNORE_TABLES)).thenReturn("");
        // SPEX ADDED: END
        @SphereEx(Type.MODIFY)
        SingleRule rule = new SingleRule(ruleConfig, "test_database", null, new HashMap<>(), Collections.emptyList(), props);
        String tableName = "test_table";
        Optional<SingleConstraintReviser> constraintReviser = reviseEntry.getConstraintReviser(rule, tableName);
        assertTrue(constraintReviser.isPresent());
        assertThat(constraintReviser.get().getClass(), is(SingleConstraintReviser.class));
    }
}
