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

package com.sphereex.dbplusengine.infra.database.oracle.metadata.database.datatype;

import com.cedarsoftware.util.CaseInsensitiveMap;
import com.sphereex.dbplusengine.infra.database.core.metadata.database.datatype.DialectDataValueConverter;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.NClob;
import java.sql.RowId;
import java.sql.SQLXML;
import java.sql.Struct;
import java.sql.Timestamp;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Data value converter of Oracle.
 */
public final class OracleDataValueConverter implements DialectDataValueConverter {
    
    private static final Map<String, Class<?>> DATA_TYPES = new CaseInsensitiveMap<>();
    
    static {
        DATA_TYPES.put("CHAR", String.class);
        DATA_TYPES.put("VARCHAR2", String.class);
        DATA_TYPES.put("NCHAR", String.class);
        DATA_TYPES.put("NVARCHAR2", String.class);
        DATA_TYPES.put("NUMBER", BigDecimal.class);
        DATA_TYPES.put("FLOAT", BigDecimal.class);
        DATA_TYPES.put("INTEGER", BigDecimal.class);
        DATA_TYPES.put("INT", BigDecimal.class);
        DATA_TYPES.put("SMALLINT", BigDecimal.class);
        DATA_TYPES.put("BINARY_FLOAT", Float.class);
        DATA_TYPES.put("BINARY_DOUBLE", Double.class);
        DATA_TYPES.put("LONG", String.class);
        DATA_TYPES.put("RAW", byte[].class);
        DATA_TYPES.put("DATE", Timestamp.class);
        DATA_TYPES.put("TIMESTAMP", Timestamp.class);
        DATA_TYPES.put("INTERVAL YEAR", Object.class);
        DATA_TYPES.put("INTERVAL DAY", Object.class);
        DATA_TYPES.put("BLOB", Blob.class);
        DATA_TYPES.put("CLOB", Clob.class);
        DATA_TYPES.put("NCLOB", NClob.class);
        DATA_TYPES.put("BFILE", Object.class);
        DATA_TYPES.put("ROWID", RowId.class);
        DATA_TYPES.put("UROWID", RowId.class);
        DATA_TYPES.put("ANYDATA", Object.class);
        DATA_TYPES.put("ANYTYPE", Object.class);
        DATA_TYPES.put("ANYDATASET", Object.class);
        DATA_TYPES.put("XMLTYPE", SQLXML.class);
        DATA_TYPES.put("URITYPE", Struct.class);
        DATA_TYPES.put("SDO_ORDINATE_ARRAY", Object.class);
        DATA_TYPES.put("SDO_ELEM_INFO_ARRAY", Object.class);
        DATA_TYPES.put("SDO_GEOMETRY", Struct.class);
        DATA_TYPES.put("SDO_TOPO_GEOMETRY", Struct.class);
    }
    
    private static final Pattern NUMBER_PRECISION = Pattern.compile("number\\s*\\(\\s*([0-9\\s]+),?\\s*([0-9-]*)\\s*\\)", Pattern.CASE_INSENSITIVE);
    
    private static final Pattern DECIMAL_PATTERN = Pattern.compile("(numeric|decimal|dec)\\s*(\\(\\s*([0-9\\s]+),?\\s*([0-9-]*)\\s*\\))?", Pattern.CASE_INSENSITIVE);
    
    @Override
    public Object covert(final Object originValue, final String dataTypeName, final String dataTypeDefinition) {
        Class<?> convertClass = DATA_TYPES.getOrDefault(dataTypeName, Object.class);
        if (originValue instanceof String) {
            return covertStringValue((String) originValue, convertClass, dataTypeDefinition.trim());
        }
        // TODO handle other types
        return originValue;
    }
    
    private Object covertStringValue(final String originValue, final Class<?> convertClass, final String dataTypeDefinition) {
        if (String.class == convertClass) {
            return originValue;
        }
        if (BigDecimal.class == convertClass) {
            Matcher numberMatcher = NUMBER_PRECISION.matcher(dataTypeDefinition);
            if (numberMatcher.matches()) {
                int scale = null == numberMatcher.group(2) || numberMatcher.group(2).isEmpty() ? 0 : Integer.parseInt(numberMatcher.group(2));
                return new BigDecimal(new BigDecimal(originValue).setScale(scale, RoundingMode.HALF_UP).stripTrailingZeros().toPlainString());
            }
            Matcher decimalMatcher = DECIMAL_PATTERN.matcher(dataTypeDefinition);
            if (decimalMatcher.matches()) {
                int scale = null == decimalMatcher.group(4) || decimalMatcher.group(4).isEmpty() ? 0 : Integer.parseInt(decimalMatcher.group(4));
                return new BigDecimal(new BigDecimal(originValue).setScale(scale, RoundingMode.HALF_UP).stripTrailingZeros().toPlainString());
            }
            if ("int".equalsIgnoreCase(dataTypeDefinition) || "integer".equalsIgnoreCase(dataTypeDefinition) || "smallint".equalsIgnoreCase(dataTypeDefinition)) {
                return new BigDecimal(new BigDecimal(originValue).setScale(0, RoundingMode.HALF_UP).stripTrailingZeros().toPlainString());
            }
            return new BigDecimal(originValue);
        }
        if (Float.class == convertClass) {
            return Float.parseFloat(originValue);
        }
        if (Double.class == convertClass) {
            return Double.parseDouble(originValue);
        }
        return originValue;
    }
    
    @Override
    public String getDatabaseType() {
        return "Oracle";
    }
}
