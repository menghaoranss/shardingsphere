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

package com.sphereex.dbplusengine.encrypt.rewrite.util;

import com.cedarsoftware.util.CaseInsensitiveMap;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.apache.shardingsphere.encrypt.rule.EncryptRule;
import org.apache.shardingsphere.infra.metadata.ShardingSphereMetaData;
import org.apache.shardingsphere.infra.metadata.database.ShardingSphereDatabase;

import java.util.Map;

/**
 * Encrypt SQL rewrite utils.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class EncryptSQLRewriteUtils {
    
    /**
     * Get database encrypt rules.
     *
     * @param metaData shardingSphere meta data
     * @return database encrypt rules
     */
    public static Map<String, EncryptRule> getDatabaseEncryptRules(final ShardingSphereMetaData metaData) {
        Map<String, EncryptRule> result = new CaseInsensitiveMap<>(metaData.getAllDatabases().size(), 1F);
        for (ShardingSphereDatabase each : metaData.getAllDatabases()) {
            each.getRuleMetaData().findSingleRule(EncryptRule.class).ifPresent(optional -> result.put(each.getName(), optional));
        }
        return result;
    }
}
