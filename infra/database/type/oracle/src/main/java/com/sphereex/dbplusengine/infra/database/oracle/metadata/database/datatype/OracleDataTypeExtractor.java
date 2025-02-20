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

import com.sphereex.dbplusengine.infra.database.core.metadata.database.datatype.DialectDataTypeExtractor;

import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Data type extractor of Oracle.
 */
public final class OracleDataTypeExtractor implements DialectDataTypeExtractor {
    
    private final Pattern intervalYear = Pattern.compile("interval\\syear[()0-9\\w\\s]+", Pattern.CASE_INSENSITIVE);
    
    private final Pattern intervalDay = Pattern.compile("interval\\sday[()0-9\\w\\s]+", Pattern.CASE_INSENSITIVE);
    
    private final Pattern characterVarying = Pattern.compile("character\\s*(varying)?[0-9()\\s]+", Pattern.CASE_INSENSITIVE);
    
    private final Pattern charPattern = Pattern.compile("char\\s+varying[0-9()\\s]+", Pattern.CASE_INSENSITIVE);
    
    private final Pattern ncharPattern = Pattern.compile("nchar\\s+varying[0-9()\\s]+", Pattern.CASE_INSENSITIVE);
    
    private final Pattern varcharPattern = Pattern.compile("varchar[0-9()\\s]+", Pattern.CASE_INSENSITIVE);
    
    private final Pattern nationalCharacter = Pattern.compile("national\\s+character\\s*(varying)?[0-9()\\s]+", Pattern.CASE_INSENSITIVE);
    
    private final Pattern nationalChar = Pattern.compile("national\\s+char\\s*(varying)?[0-9()\\s]+", Pattern.CASE_INSENSITIVE);
    
    private final Pattern doublePrecision = Pattern.compile("double\\s+precision", Pattern.CASE_INSENSITIVE);
    
    private final Pattern longRaw = Pattern.compile("long\\s+raw", Pattern.CASE_INSENSITIVE);
    
    private final Pattern sysDotType = Pattern.compile("sys\\.(\\w+)", Pattern.CASE_INSENSITIVE);
    
    @Override
    public Optional<String> extract(final String dataTypeDefinition) {
        if (intervalYear.matcher(dataTypeDefinition).matches()) {
            return Optional.of("INTERVAL YEAR");
        }
        if (intervalDay.matcher(dataTypeDefinition).matches()) {
            return Optional.of("INTERVAL DAY");
        }
        if (characterVarying.matcher(dataTypeDefinition).matches() || charPattern.matcher(dataTypeDefinition).matches() || varcharPattern.matcher(dataTypeDefinition).matches()) {
            return Optional.of("VARCHAR2");
        }
        if (ncharPattern.matcher(dataTypeDefinition).matches() || nationalChar.matcher(dataTypeDefinition).matches()) {
            return Optional.of("NVARCHAR2");
        }
        if (nationalCharacter.matcher(dataTypeDefinition).matches()) {
            return Optional.of("NCHAR");
        }
        if (dataTypeDefinition.toLowerCase().startsWith("numeric") || dataTypeDefinition.toLowerCase().startsWith("dec")) {
            return Optional.of("NUMBER");
        }
        if (doublePrecision.matcher(dataTypeDefinition).matches() || "real".equalsIgnoreCase(dataTypeDefinition)) {
            return Optional.of("FLOAT");
        }
        if (longRaw.matcher(dataTypeDefinition).matches()) {
            return Optional.of("LONG RAW");
        }
        Matcher matcher = sysDotType.matcher(dataTypeDefinition);
        if (matcher.matches()) {
            return Optional.of(matcher.group(1));
        }
        return Optional.empty();
    }
    
    @Override
    public String getDatabaseType() {
        return "Oracle";
    }
}
