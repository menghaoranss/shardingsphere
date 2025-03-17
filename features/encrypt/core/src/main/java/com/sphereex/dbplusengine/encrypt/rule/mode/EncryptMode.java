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

package com.sphereex.dbplusengine.encrypt.rule.mode;

import com.sphereex.dbplusengine.encrypt.config.rule.mode.EncryptModeRuleConfiguration;
import com.sphereex.dbplusengine.encrypt.config.rule.mode.EncryptModeType;
import com.sphereex.dbplusengine.encrypt.constant.EncryptModeKey;
import lombok.Getter;

import java.util.Optional;
import java.util.Properties;

/**
 * Encrypt mode.
 */
@Getter
public final class EncryptMode {
    
    private final EncryptModeType type;
    
    private final boolean udfSQLEnabled;
    
    private final boolean udfRoutineEnabled;
    
    private final boolean udfViewEnabled;
    
    private final boolean useOriginalSQLWhenCipherQueryFailed;
    
    private final String renameTablePrefix;
    
    private final String derivedCipherSuffix;
    
    private final String derivedAssistedQuerySuffix;
    
    private final String derivedLikeQuerySuffix;
    
    private final String derivedOrderQuerySuffix;
    
    public EncryptMode(final EncryptModeRuleConfiguration encryptMode) {
        type = null == encryptMode ? EncryptModeType.FRONTEND : encryptMode.getType();
        Properties props = null == encryptMode ? new Properties() : encryptMode.getProps();
        udfSQLEnabled = EncryptModeType.FRONTEND == type && parseBoolean(props, EncryptModeKey.UDF_SQL_ENABLED_KEY);
        udfRoutineEnabled = EncryptModeType.FRONTEND == type && parseBoolean(props, EncryptModeKey.UDF_ROUTINE_ENABLED_KEY);
        udfViewEnabled = EncryptModeType.FRONTEND == type && parseBoolean(props, EncryptModeKey.UDF_VIEW_ENABLED_KEY);
        useOriginalSQLWhenCipherQueryFailed = EncryptModeType.FRONTEND == type && parseBoolean(props, EncryptModeKey.USE_ORIGINAL_SQL_WHEN_CIPHER_QUERY_FAILED_KEY);
        derivedCipherSuffix = EncryptModeType.FRONTEND == type ? props.getProperty(EncryptModeKey.DERIVED_CIPHER_SUFFIX_KEY) : null;
        derivedAssistedQuerySuffix = EncryptModeType.FRONTEND == type ? props.getProperty(EncryptModeKey.DERIVED_ASSISTED_QUERY_SUFFIX_KEY) : null;
        derivedLikeQuerySuffix = EncryptModeType.FRONTEND == type ? props.getProperty(EncryptModeKey.DERIVED_LIKE_QUERY_SUFFIX_KEY) : null;
        derivedOrderQuerySuffix = EncryptModeType.FRONTEND == type ? props.getProperty(EncryptModeKey.DERIVED_ORDER_QUERY_SUFFIX_KEY) : null;
        renameTablePrefix = EncryptModeType.BACKEND == type ? props.getProperty(EncryptModeKey.RENAME_TABLE_PREFIX_KEY, "SPEX_") : null;
    }
    
    private boolean parseBoolean(final Properties props, final String key) {
        return Boolean.parseBoolean(props.getOrDefault(key, "false").toString());
    }
    
    /**
     * Get rename table prefix.
     *
     * @return rename table prefix
     */
    public Optional<String> getRenameTablePrefix() {
        return Optional.ofNullable(renameTablePrefix);
    }
    
    /**
     * Judge whether UDF is enabled or not.
     *
     * @return UDF is enabled or not
     */
    public boolean isUDFEnabled() {
        return udfSQLEnabled || udfRoutineEnabled || udfViewEnabled || EncryptModeType.BACKEND == type;
    }
}
