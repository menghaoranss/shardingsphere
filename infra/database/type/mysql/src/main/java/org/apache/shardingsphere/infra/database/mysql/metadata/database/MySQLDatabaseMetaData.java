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

package org.apache.shardingsphere.infra.database.mysql.metadata.database;

import org.apache.shardingsphere.infra.database.core.metadata.database.DialectDatabaseMetaData;
import org.apache.shardingsphere.infra.database.core.metadata.database.enums.NullsOrderType;
import org.apache.shardingsphere.infra.database.core.metadata.database.enums.QuoteCharacter;
import org.apache.shardingsphere.infra.database.core.metadata.database.option.DialectDatabaseJoinOrderOption;
import org.apache.shardingsphere.infra.database.core.metadata.database.option.DialectDatabaseTransactionOption;

import java.math.BigInteger;
import java.sql.Types;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Database meta data of MySQL.
 */
public final class MySQLDatabaseMetaData implements DialectDatabaseMetaData {
    
    @Override
    public QuoteCharacter getQuoteCharacter() {
        return QuoteCharacter.BACK_QUOTE;
    }
    
    @Override
    public Map<String, Integer> getExtraDataTypes() {
        Map<String, Integer> result = new HashMap<>(10, 1F);
        result.put("JSON", Types.LONGVARCHAR);
        result.put("GEOMETRY", Types.BINARY);
        result.put("GEOMETRYCOLLECTION", Types.BINARY);
        result.put("YEAR", Types.DATE);
        result.put("POINT", Types.BINARY);
        result.put("MULTIPOINT", Types.BINARY);
        result.put("POLYGON", Types.BINARY);
        result.put("MULTIPOLYGON", Types.BINARY);
        result.put("LINESTRING", Types.BINARY);
        result.put("MULTILINESTRING", Types.BINARY);
        return result;
    }
    
    @Override
    public Optional<Class<?>> findExtraSQLTypeClass(final int dataType, final boolean unsigned) {
        if (Types.TINYINT == dataType || Types.SMALLINT == dataType) {
            return Optional.of(Integer.class);
        }
        if (Types.INTEGER == dataType) {
            return unsigned ? Optional.of(Long.class) : Optional.of(Integer.class);
        }
        if (Types.BIGINT == dataType) {
            return unsigned ? Optional.of(BigInteger.class) : Optional.of(Long.class);
        }
        return Optional.empty();
    }
    
    @Override
    public NullsOrderType getDefaultNullsOrderType() {
        return NullsOrderType.LOW;
    }
    
    @Override
    public boolean isInstanceConnectionAvailable() {
        return true;
    }
    
    @Override
    public boolean isSupportThreeTierStorageStructure() {
        return true;
    }
    
    @Override
    public DialectDatabaseTransactionOption getTransactionOption() {
        return new DialectDatabaseTransactionOption(false, false, true, false, true);
    }
    
    @Override
    public DialectDatabaseJoinOrderOption getJoinOrderOption() {
        return new DialectDatabaseJoinOrderOption(true, true);
    }
    
    @Override
    public String getDatabaseType() {
        return "MySQL";
    }
}
