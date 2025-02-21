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

package com.sphereex.dbplusengine.mask.algorithm;

import com.google.common.base.Splitter;
import com.sphereex.dbplusengine.mask.spi.MaskMatchingAlgorithm;
import org.apache.shardingsphere.infra.algorithm.core.exception.AlgorithmInitializationException;
import org.apache.shardingsphere.infra.exception.core.ShardingSpherePreconditions;
import org.apache.shardingsphere.infra.metadata.user.Grantee;

import java.util.Collection;
import java.util.HashSet;
import java.util.Properties;

/**
 * SphereEx role mask matching algorithm.
 */
public final class SphereExRoleMaskMatchingAlgorithm implements MaskMatchingAlgorithm {
    
    private static final String ROLE_LISTS = "role-lists";
    
    private Collection<String> roleLists;
    
    @Override
    public void init(final Properties props) {
        ShardingSpherePreconditions.checkContainsKey(props, ROLE_LISTS, () -> new AlgorithmInitializationException(this, "%s is required", ROLE_LISTS));
        roleLists = new HashSet<>(Splitter.on(",").trimResults().splitToList(props.getProperty(ROLE_LISTS)));
    }
    
    @Override
    public boolean matched(final Grantee grantee, final Collection<String> roles) {
        for (String each : roles) {
            if (roleLists.contains(each)) {
                return true;
            }
        }
        return false;
    }
    
    @Override
    public String getType() {
        return "SphereEx:MASK_ROLE";
    }
}
