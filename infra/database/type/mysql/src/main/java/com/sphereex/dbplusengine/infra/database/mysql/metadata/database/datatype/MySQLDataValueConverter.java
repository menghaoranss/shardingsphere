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

package com.sphereex.dbplusengine.infra.database.mysql.metadata.database.datatype;

import com.cedarsoftware.util.CaseInsensitiveMap;
import com.sphereex.dbplusengine.infra.database.core.metadata.database.datatype.DialectDataValueConverter;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Data value converter of MySQL.
 */
public final class MySQLDataValueConverter implements DialectDataValueConverter {
    
    private static final Map<String, Class<?>> DATA_TYPES = new CaseInsensitiveMap<>();
    
    static {
        DATA_TYPES.put("INT", Integer.class);
        DATA_TYPES.put("BIT", byte[].class);
        DATA_TYPES.put("TINYINT", Integer.class);
        DATA_TYPES.put("SMALLINT", Integer.class);
        DATA_TYPES.put("MEDIUMINT", Integer.class);
        DATA_TYPES.put("BIGINT", Long.class);
        DATA_TYPES.put("DECIMAL", BigDecimal.class);
        DATA_TYPES.put("FLOAT", Float.class);
        DATA_TYPES.put("DOUBLE", Double.class);
        DATA_TYPES.put("DATE", Date.class);
        DATA_TYPES.put("DATETIME", Timestamp.class);
        DATA_TYPES.put("TIME", Time.class);
        DATA_TYPES.put("YEAR", Date.class);
        DATA_TYPES.put("CHAR", String.class);
        DATA_TYPES.put("VARCHAR", String.class);
        DATA_TYPES.put("BINARY", byte[].class);
        DATA_TYPES.put("VARBINARY", byte[].class);
        DATA_TYPES.put("TINYBLOB", byte[].class);
        DATA_TYPES.put("TINYTEXT", String.class);
        DATA_TYPES.put("TEXT", String.class);
        DATA_TYPES.put("MEDIUMBLOB", byte[].class);
        DATA_TYPES.put("MEDIUMTEXT", String.class);
        DATA_TYPES.put("LONGBLOB", byte[].class);
        DATA_TYPES.put("LONGTEXT", String.class);
        DATA_TYPES.put("ENUM", String.class);
        DATA_TYPES.put("SET", String.class);
        DATA_TYPES.put("GEOMETRY", byte[].class);
        DATA_TYPES.put("POINT", byte[].class);
        DATA_TYPES.put("LINESTRING", byte[].class);
        DATA_TYPES.put("POLYGON", byte[].class);
        DATA_TYPES.put("MULTIPOINT", byte[].class);
        DATA_TYPES.put("MULTILINESTRING", byte[].class);
        DATA_TYPES.put("MULTIPOLYGON", byte[].class);
        DATA_TYPES.put("GEOMETRYCOLLECTION", byte[].class);
        DATA_TYPES.put("JSON", String.class);
    }
    
    private static final Pattern INT_UNSIGNED = Pattern.compile("(int|integer)\\s*[()0-9\\s]*unsigned\\s*(zerofill)?.*", Pattern.CASE_INSENSITIVE);
    
    private static final Pattern BIGINT_UNSIGNED = Pattern.compile("bigint\\s*[()0-9\\s]*unsigned\\s*(zerofill)?.*", Pattern.CASE_INSENSITIVE);
    
    private static final Pattern DECIMAL_PRECISION = Pattern.compile("(decimal|dec|numeric|fixed)(\\s*\\(\\s*([0-9\\s]+),?\\s*([0-9-]*)\\s*\\))?.*", Pattern.CASE_INSENSITIVE);
    
    private static final Pattern FLOAT_PRECISION = Pattern.compile("float\\s*\\(\\s*([0-9\\s]+),\\s*([0-9-]+)\\s*\\).*", Pattern.CASE_INSENSITIVE);
    
    private static final Pattern FLOAT_PRECISION_WITHOUT_SCALE = Pattern.compile("float\\s*\\(\\s*([0-9]+)\\s*\\).*", Pattern.CASE_INSENSITIVE);
    
    private static final Pattern DOUBLE_PRECISION = Pattern.compile("(double|double\\s+precision|real)\\s*\\([0-9\\s]*,\\s*([0-9]*)\\s*\\).*", Pattern.CASE_INSENSITIVE);
    
    @Override
    public Object covert(final Object originValue, final String dataTypeName, final String dataTypeDefinition) {
        Class<?> convertClass = DATA_TYPES.getOrDefault(dataTypeName, Object.class);
        if (originValue instanceof String) {
            return covertStringValue((String) originValue, convertClass, dataTypeDefinition);
        }
        // TODO handle other types
        return originValue;
    }
    
    private Object covertStringValue(final String originValue, final Class<?> convertClass, final String dataTypeDefinition) {
        if (String.class == convertClass) {
            return originValue;
        }
        if (Integer.class == convertClass) {
            if (INT_UNSIGNED.matcher(dataTypeDefinition).matches()) {
                return Long.parseLong(originValue);
            }
            return Integer.parseInt(originValue);
        }
        if (Long.class == convertClass) {
            if (BIGINT_UNSIGNED.matcher(dataTypeDefinition).matches()) {
                return new BigInteger(originValue);
            }
            return Long.parseLong(originValue);
        }
        if (BigDecimal.class == convertClass) {
            Matcher matcher = DECIMAL_PRECISION.matcher(dataTypeDefinition);
            if (matcher.matches()) {
                int scale = null == matcher.group(4) || matcher.group(4).isEmpty() ? 0 : Integer.parseInt(matcher.group(4));
                return new BigDecimal(originValue).setScale(scale, RoundingMode.HALF_UP);
            }
            return new BigDecimal(originValue);
        }
        if (Float.class == convertClass) {
            return handleFloat(originValue, dataTypeDefinition);
        }
        if (Double.class == convertClass) {
            Matcher matcher = DOUBLE_PRECISION.matcher(dataTypeDefinition);
            if (matcher.matches()) {
                return new BigDecimal(originValue).setScale(Integer.parseInt(matcher.group(2)), RoundingMode.HALF_UP).doubleValue();
            }
            return Double.parseDouble(originValue);
        }
        return originValue;
    }
    
    private Object handleFloat(final String originValue, final String dataTypeDefinition) {
        Matcher floatPrecisionMatcher = FLOAT_PRECISION.matcher(dataTypeDefinition);
        if (floatPrecisionMatcher.matches()) {
            return new BigDecimal(originValue).setScale(Integer.parseInt(floatPrecisionMatcher.group(2)), RoundingMode.HALF_UP).floatValue();
        }
        Matcher floatPrecisionWithoutScaleMatcher = FLOAT_PRECISION_WITHOUT_SCALE.matcher(dataTypeDefinition);
        if (floatPrecisionWithoutScaleMatcher.matches()) {
            if (Integer.parseInt(floatPrecisionWithoutScaleMatcher.group(1)) > 24) {
                return Double.parseDouble(originValue);
            } else {
                return Float.parseFloat(originValue);
            }
        }
        return Float.parseFloat(originValue);
    }
    
    @Override
    public String getDatabaseType() {
        return "MySQL";
    }
}
