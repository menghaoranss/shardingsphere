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

import com.sphereex.dbplusengine.infra.database.core.metadata.database.datatype.DialectDataTypeExtractor;

import java.util.Optional;
import java.util.regex.Pattern;

/**
 * Data type extractor of MySQL.
 */
public final class MySQLDataTypeExtractor implements DialectDataTypeExtractor {
    
    private final Pattern nationalChar = Pattern.compile("national\\s+char.*", Pattern.CASE_INSENSITIVE);
    
    private final Pattern nationalVarchar = Pattern.compile("national\\s+varchar.*", Pattern.CASE_INSENSITIVE);
    
    private final Pattern doublePrecision = Pattern.compile("double\\s+precision.*", Pattern.CASE_INSENSITIVE);
    
    @Override
    public Optional<String> extract(final String dataTypeDefinition) {
        if ("bool".equalsIgnoreCase(dataTypeDefinition) || "boolean".equalsIgnoreCase(dataTypeDefinition)) {
            return Optional.of("TINYINT");
        }
        if (dataTypeDefinition.toLowerCase().startsWith("int")) {
            return Optional.of("INT");
        }
        if (dataTypeDefinition.toLowerCase().startsWith("dec") || dataTypeDefinition.toLowerCase().startsWith("numeric") || dataTypeDefinition.toLowerCase().startsWith("fixed")) {
            return Optional.of("DECIMAL");
        }
        if (nationalChar.matcher(dataTypeDefinition).matches()) {
            return Optional.of("CHAR");
        }
        if (nationalVarchar.matcher(dataTypeDefinition).matches()) {
            return Optional.of("VARCHAR");
        }
        if (dataTypeDefinition.toLowerCase().startsWith("real")) {
            return Optional.of("DOUBLE");
        }
        if (doublePrecision.matcher(dataTypeDefinition).matches()) {
            return Optional.of("DOUBLE");
        }
        return Optional.empty();
    }
    
    @Override
    public String getDatabaseType() {
        return "MySQL";
    }
}
