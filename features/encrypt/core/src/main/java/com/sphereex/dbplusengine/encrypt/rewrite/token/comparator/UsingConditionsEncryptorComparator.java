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

package com.sphereex.dbplusengine.encrypt.rewrite.token.comparator;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.apache.shardingsphere.encrypt.rewrite.token.comparator.EncryptorComparator;
import org.apache.shardingsphere.encrypt.rule.EncryptRule;
import org.apache.shardingsphere.sql.parser.statement.core.segment.dml.column.ColumnSegment;
import org.apache.shardingsphere.sql.parser.statement.core.segment.generic.bound.ColumnSegmentBoundInfo;

import java.util.Collection;
import java.util.Map;

/**
 * Using conditions encryptor comparator.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class UsingConditionsEncryptorComparator {
    
    /**
     * Judge whether all using columns use same encryptor or not.
     *
     * @param usingColumns using columns
     * @param rule encrypt rule
     * @param databaseEncryptRules database encrypt rules
     * @return same or different encryptors are using
     */
    public static boolean isSame(final Collection<ColumnSegment> usingColumns, final EncryptRule rule, final Map<String, EncryptRule> databaseEncryptRules) {
        for (ColumnSegment each : usingColumns) {
            ColumnSegmentBoundInfo leftColumnInfo = each.getColumnBoundInfo();
            ColumnSegmentBoundInfo rightColumnInfo = each.getOtherUsingColumnBoundInfo();
            if (!EncryptorComparator.isSame(rule, leftColumnInfo, rightColumnInfo, databaseEncryptRules)) {
                return false;
            }
        }
        return true;
    }
}
