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

package org.apache.shardingsphere.encrypt.config.rule;

import com.sphereex.dbplusengine.SphereEx;
import com.sphereex.dbplusengine.encrypt.config.rule.PlainColumnItemRuleConfiguration;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;

import java.util.Optional;

/**
 * Encrypt column rule configuration.
 */
@RequiredArgsConstructor
@Getter
@Setter
public final class EncryptColumnRuleConfiguration {
    
    private final String name;
    
    private final EncryptColumnItemRuleConfiguration cipher;
    
    private EncryptColumnItemRuleConfiguration assistedQuery;
    
    private EncryptColumnItemRuleConfiguration likeQuery;
    
    @SphereEx
    private EncryptColumnItemRuleConfiguration orderQuery;
    
    @SphereEx
    private PlainColumnItemRuleConfiguration plain;
    
    @SphereEx
    private String dataType;
    
    /**
     * Get assisted query.
     *
     * @return assisted query column item rule configuration
     */
    public Optional<EncryptColumnItemRuleConfiguration> getAssistedQuery() {
        return Optional.ofNullable(assistedQuery);
    }
    
    /**
     * Get like query.
     *
     * @return like query column item rule configuration
     */
    public Optional<EncryptColumnItemRuleConfiguration> getLikeQuery() {
        return Optional.ofNullable(likeQuery);
    }
    
    /**
     * Get order query.
     *
     * @return order query column item rule configuration
     */
    @SphereEx
    public Optional<EncryptColumnItemRuleConfiguration> getOrderQuery() {
        return Optional.ofNullable(orderQuery);
    }
    
    /**
     * Get plain.
     *
     * @return plain column item rule configuration
     */
    @SphereEx
    public Optional<PlainColumnItemRuleConfiguration> getPlain() {
        return Optional.ofNullable(plain);
    }
    
    /**
     * Get data type.
     *
     * @return data type
     */
    @SphereEx
    public Optional<String> getDataType() {
        return Optional.ofNullable(dataType);
    }
}
