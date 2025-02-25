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

package com.sphereex.dbplusengine.encrypt.config.rule.compatible;

import com.sphereex.dbplusengine.encrypt.config.rule.PlainColumnItemRuleConfiguration;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.apache.shardingsphere.encrypt.config.rule.EncryptColumnItemRuleConfiguration;
import org.apache.shardingsphere.encrypt.config.rule.EncryptColumnRuleConfiguration;

/**
 * Compatible encrypt column rule configuration.
 */
@RequiredArgsConstructor
@Getter
public final class CompatibleEncryptColumnRuleConfiguration {
    
    private final String logicColumn;
    
    private final String cipherColumn;
    
    private final String assistedQueryColumn;
    
    private final String likeQueryColumn;
    
    private final String orderQueryColumn;
    
    private final String plainColumn;
    
    private final String encryptorName;
    
    private final String assistedQueryEncryptorName;
    
    private final String likeQueryEncryptorName;
    
    private final String orderQueryEncryptorName;
    
    private final Boolean queryWithCipherColumn;
    
    /**
     * Convert to encrypt column rule configuration.
     *
     * @param tableQueryWithCipherColumn table query with cipher column
     * @return encrypt column rule configuration
     */
    public EncryptColumnRuleConfiguration toEncryptColumnRuleConfig(final boolean tableQueryWithCipherColumn) {
        EncryptColumnRuleConfiguration result = new EncryptColumnRuleConfiguration(logicColumn, new EncryptColumnItemRuleConfiguration(cipherColumn, encryptorName));
        if (null != assistedQueryColumn && null != assistedQueryEncryptorName) {
            result.setAssistedQuery(new EncryptColumnItemRuleConfiguration(assistedQueryColumn, assistedQueryEncryptorName));
        }
        if (null != likeQueryColumn && null != likeQueryEncryptorName) {
            result.setLikeQuery(new EncryptColumnItemRuleConfiguration(likeQueryColumn, likeQueryEncryptorName));
        }
        if (null != orderQueryColumn && null != orderQueryEncryptorName) {
            result.setOrderQuery(new EncryptColumnItemRuleConfiguration(orderQueryColumn, orderQueryEncryptorName));
        }
        if (null != plainColumn) {
            boolean columnQueryWithCipherColumn = null == queryWithCipherColumn ? tableQueryWithCipherColumn : queryWithCipherColumn;
            PlainColumnItemRuleConfiguration plain = new PlainColumnItemRuleConfiguration(plainColumn);
            plain.setQueryWithPlain(!columnQueryWithCipherColumn);
            result.setPlain(plain);
        }
        return result;
    }
}
