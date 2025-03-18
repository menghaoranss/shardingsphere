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

package org.apache.shardingsphere.encrypt.enums;

import com.google.common.base.Strings;
import com.sphereex.dbplusengine.SphereEx;
import com.sphereex.dbplusengine.encrypt.rule.mode.EncryptMode;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import org.apache.shardingsphere.infra.database.core.metadata.database.DialectDatabaseMetaData;
import org.apache.shardingsphere.infra.database.core.type.DatabaseType;
import org.apache.shardingsphere.infra.database.core.type.DatabaseTypeRegistry;

/**
 * Encrypt derived column suffix.
 */
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public enum EncryptDerivedColumnSuffix {
    
    CIPHER("_CIPHER"),
    ASSISTED_QUERY("_ASSISTED"),
    LIKE_QUERY("_LIKE"),
    @SphereEx
    ORDER_QUERY("_ORDER");
    
    private final String suffix;
    
    /**
     * Get derived column name.
     *
     * @param columnName column name
     * @param databaseType database type
     * @param encryptMode encrypt mode
     * @return derived column name
     */
    public String getDerivedColumnName(final String columnName, final DatabaseType databaseType, @SphereEx final EncryptMode encryptMode) {
        DialectDatabaseMetaData dialectDatabaseMetaData = new DatabaseTypeRegistry(databaseType).getDialectDatabaseMetaData();
        // SPEX CHANGED: BEGIN
        return String.format("%s%s", columnName, dialectDatabaseMetaData.formatTableNamePattern(getSuffix(encryptMode)));
        // SPEX CHANGED: END
    }
    
    @SphereEx
    private String getSuffix(final EncryptMode encryptMode) {
        if (CIPHER == this && !Strings.isNullOrEmpty(encryptMode.getDerivedCipherSuffix())) {
            return encryptMode.getDerivedCipherSuffix();
        }
        if (ASSISTED_QUERY == this && !Strings.isNullOrEmpty(encryptMode.getDerivedAssistedQuerySuffix())) {
            return encryptMode.getDerivedAssistedQuerySuffix();
        }
        if (LIKE_QUERY == this && !Strings.isNullOrEmpty(encryptMode.getDerivedLikeQuerySuffix())) {
            return encryptMode.getDerivedLikeQuerySuffix();
        }
        if (ORDER_QUERY == this && !Strings.isNullOrEmpty(encryptMode.getDerivedOrderQuerySuffix())) {
            return encryptMode.getDerivedOrderQuerySuffix();
        }
        return suffix;
    }
}
