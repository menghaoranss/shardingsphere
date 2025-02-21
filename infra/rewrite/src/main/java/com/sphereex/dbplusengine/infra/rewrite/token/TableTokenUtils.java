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

package com.sphereex.dbplusengine.infra.rewrite.token;

import org.apache.shardingsphere.infra.database.core.type.DatabaseType;
import org.apache.shardingsphere.infra.database.core.type.DatabaseTypeRegistry;
import org.apache.shardingsphere.infra.metadata.database.resource.unit.StorageUnit;
import org.apache.shardingsphere.sql.parser.statement.core.segment.generic.OwnerSegment;
import org.apache.shardingsphere.sql.parser.statement.core.segment.generic.table.SimpleTableSegment;
import org.apache.shardingsphere.sql.parser.statement.core.value.identifier.IdentifierValue;

import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;

/**
 * Table token utils.
 */
public class TableTokenUtils {
    
    /**
     * Whether is need schema rewrite.
     *
     * @param storageUnits storage units
     * @return true if need schema rewrite, false otherwise
     */
    public static boolean isNeedSchemaRewrite(final Map<String, StorageUnit> storageUnits) {
        for (Entry<String, StorageUnit> entry : storageUnits.entrySet()) {
            if (new DatabaseTypeRegistry(entry.getValue().getStorageType()).getDialectDatabaseMetaData().isInstanceConnectionAvailable()) {
                return true;
            }
        }
        return false;
    }
    
    /**
     * Get database name.
     *
     * @param tableSegment table segment
     * @param databaseType database type
     * @return database name
     */
    public static Optional<String> getDatabaseName(final SimpleTableSegment tableSegment, final DatabaseType databaseType) {
        if (new DatabaseTypeRegistry(databaseType).getDialectDatabaseMetaData().isSchemaAvailable()) {
            return tableSegment.getOwner().flatMap(OwnerSegment::getOwner).map(OwnerSegment::getIdentifier).map(IdentifierValue::getValue);
        } else {
            return tableSegment.getOwner().map(OwnerSegment::getIdentifier).map(IdentifierValue::getValue);
        }
    }
}
