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

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.apache.shardingsphere.encrypt.config.rule.EncryptColumnRuleConfiguration;
import org.apache.shardingsphere.encrypt.config.rule.EncryptTableRuleConfiguration;

import java.util.Collection;
import java.util.LinkedList;

/**
 * Compatible encrypt table rule configuration.
 */
@RequiredArgsConstructor
@Getter
public final class CompatibleEncryptTableRuleConfiguration {
    
    private final String name;
    
    private final Collection<CompatibleEncryptColumnRuleConfiguration> columns;
    
    private final Boolean queryWithCipherColumn;
    
    /**
     * Convert to encrypt table rule configuration.
     *
     * @param globalQueryWithCipherColumn global query with cipher column
     * @return encrypt table rule configuration
     */
    public EncryptTableRuleConfiguration toEncryptTableRuleConfig(final boolean globalQueryWithCipherColumn) {
        Collection<EncryptColumnRuleConfiguration> columns = new LinkedList<>();
        boolean tableQueryWithCipherColumn = null == queryWithCipherColumn ? globalQueryWithCipherColumn : queryWithCipherColumn;
        this.columns.forEach(each -> columns.add(each.toEncryptColumnRuleConfig(tableQueryWithCipherColumn)));
        return new EncryptTableRuleConfiguration(name, columns);
    }
}
