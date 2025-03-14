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

package com.sphereex.dbplusengine.encrypt.context;

import com.sphereex.dbplusengine.SphereEx;
import com.sphereex.dbplusengine.infra.hint.EncryptColumnItemType;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.apache.shardingsphere.encrypt.rule.column.EncryptColumn;
import org.apache.shardingsphere.infra.database.core.type.DatabaseType;

/**
 * Encrypt context builder.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class EncryptContextBuilder {
    
    /**
     * Build context.
     *
     * @param column encrypt column
     * @param databaseType database type
     * @param encryptColumnItemType encrypt column item type
     * @return encrypt context
     */
    public static EncryptContext build(final EncryptColumn column, @SphereEx final DatabaseType databaseType, @SphereEx final EncryptColumnItemType encryptColumnItemType) {
        EncryptContext result = new EncryptContext(EncryptColumnDataTypeContextBuilder.build(column), databaseType);
        // SPEX ADDED: BEGIN
        column.getAolianColumn().ifPresent(result::setAolianColumn);
        column.getDbid().ifPresent(result::setDbid);
        result.setEncryptColumnItemType(encryptColumnItemType);
        // SPEX ADDED: END
        return result;
    }
}
