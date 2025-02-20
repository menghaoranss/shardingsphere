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

package com.sphereex.dbplusengine.infra.util;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.apache.shardingsphere.infra.database.core.type.DatabaseType;

/**
 * Database type utility class.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class DatabaseTypeUtils {
    
    /**
     * Get trunk database type.
     *
     * @param databaseType database type
     * @return trunk database type
     */
    public static DatabaseType getTrunkDatabaseType(final DatabaseType databaseType) {
        return databaseType.getTrunkDatabaseType().orElse(databaseType);
    }
    
    /**
     * Check if database type is Hive or Presto.
     *
     * @param databaseType database type
     * @return whether the database type is Hive or Presto
     */
    public static boolean isHiveOrPrestoDatabase(final DatabaseType databaseType) {
        return isHiveDatabase(databaseType) || isPrestoDatabase(databaseType);
    }
    
    /**
     * Check if database type is Hive.
     *
     * @param databaseType database type
     * @return whether the database type is Hive
     */
    public static boolean isHiveDatabase(final DatabaseType databaseType) {
        return null != databaseType && "Hive".equals(databaseType.getType());
    }
    
    /**
     * Check if database type is Presto.
     *
     * @param databaseType database type
     * @return whether the database type is Presto
     */
    public static boolean isPrestoDatabase(final DatabaseType databaseType) {
        return null != databaseType && "Presto".equals(databaseType.getType());
    }
    
    /**
     * Check if database type is Oracle.
     *
     * @param databaseType database type
     * @return whether the database type is Oracle
     */
    public static boolean isOracleDatabase(final DatabaseType databaseType) {
        return null != databaseType && ("Oracle".equals(databaseType.getType()) || databaseType.getTrunkDatabaseType().map(DatabaseType::getType).map("Oracle"::equals).orElse(false));
    }
    
    /**
     * Check if database type is branch of specified trunk database type.
     *
     * @param databaseType database type
     * @param trunkDatabaseType trunk database type
     * @return whether the database type is branch of trunk database type
     */
    public static boolean isBranchType(final DatabaseType databaseType, final String trunkDatabaseType) {
        return databaseType.getType().equals(trunkDatabaseType) || databaseType.getTrunkDatabaseType().map(DatabaseType::getType).map(optional -> optional.equals(trunkDatabaseType)).orElse(false);
    }
}
