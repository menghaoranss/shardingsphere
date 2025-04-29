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

package com.sphereex.dbplusengine.infra.database.core.metadata.database.datatype;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.apache.shardingsphere.infra.database.core.metadata.database.datatype.DataTypeRegistry;
import org.apache.shardingsphere.infra.database.core.type.DatabaseType;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Data type java class converter.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class DataTypeJavaClassConverter {
    
    private static final Map<Integer, Class<?>> DATE_TYPE_JAVA_CLASSES = new HashMap<>(14, 1F);
    
    static {
        DATE_TYPE_JAVA_CLASSES.put(Types.INTEGER, Integer.class);
        DATE_TYPE_JAVA_CLASSES.put(Types.BIGINT, Long.class);
        DATE_TYPE_JAVA_CLASSES.put(Types.SMALLINT, Short.class);
        DATE_TYPE_JAVA_CLASSES.put(Types.TINYINT, Byte.class);
        DATE_TYPE_JAVA_CLASSES.put(Types.REAL, Float.class);
        DATE_TYPE_JAVA_CLASSES.put(Types.DOUBLE, Double.class);
        DATE_TYPE_JAVA_CLASSES.put(Types.BOOLEAN, Boolean.class);
        DATE_TYPE_JAVA_CLASSES.put(Types.VARBINARY, byte[].class);
        DATE_TYPE_JAVA_CLASSES.put(Types.VARCHAR, String.class);
        DATE_TYPE_JAVA_CLASSES.put(Types.CHAR, Character.class);
        DATE_TYPE_JAVA_CLASSES.put(Types.TIMESTAMP, Timestamp.class);
        DATE_TYPE_JAVA_CLASSES.put(Types.DATE, Date.class);
        DATE_TYPE_JAVA_CLASSES.put(Types.TIME, Time.class);
        DATE_TYPE_JAVA_CLASSES.put(Types.DECIMAL, BigDecimal.class);
        DATE_TYPE_JAVA_CLASSES.put(Types.NUMERIC, BigDecimal.class);
    }
    
    /**
     * Get data type java class.
     *
     * @param databaseType database type
     * @param dataType data type
     * @return data type java class
     */
    public static Optional<Class<?>> getDataTypeJavaClass(final DatabaseType databaseType, final String dataType) {
        return DataTypeRegistry.getDataType(databaseType.getType(), dataType).map(DATE_TYPE_JAVA_CLASSES::get);
    }
}
