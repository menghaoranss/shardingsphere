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
import org.apache.commons.lang3.tuple.Pair;
import org.apache.shardingsphere.infra.database.core.spi.DatabaseTypedSPILoader;
import org.apache.shardingsphere.infra.database.core.type.DatabaseType;

import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Data type extractor.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class DataTypeExtractor {
    
    private static final Pattern DECIMAL_PATTERN = Pattern.compile("DECIMAL\\((\\d+),(\\d+)\\)", Pattern.CASE_INSENSITIVE);
    
    private static final Pattern DECIMAL_WITHOUT_SCALE_PATTERN = Pattern.compile("DECIMAL\\((\\d+)\\)", Pattern.CASE_INSENSITIVE);
    
    private static final Pattern NUMERIC_PATTERN = Pattern.compile("NUMERIC\\((\\d+),(\\d+)\\)", Pattern.CASE_INSENSITIVE);
    
    private static final Pattern NUMERIC_WITHOUT_SCALE_PATTERN = Pattern.compile("NUMERIC\\((\\d+)\\)", Pattern.CASE_INSENSITIVE);
    
    /**
     * Extract data type.
     *
     * @param dataTypeDefinition data type definition
     * @param databaseType database type
     * @return extracted data type
     */
    public static String extract(final String dataTypeDefinition, final DatabaseType databaseType) {
        String trimmedDataTypeDefinition = dataTypeDefinition.trim();
        return extractDialectDataTypeName(trimmedDataTypeDefinition, databaseType).orElseGet(() -> extractStandardDataTypeName(trimmedDataTypeDefinition));
    }
    
    /**
     * Extract data length.
     *
     * @param dataTypeDefinition data type definition
     * @param databaseType database type
     * @return extracted data length
     */
    public static Optional<Pair<Integer, Integer>> extractPrecisionAndScale(final String dataTypeDefinition, final DatabaseType databaseType) {
        String trimmedDataTypeDefinition = dataTypeDefinition.replaceAll("\\s+", "");
        Optional<Pair<Integer, Integer>> result = extractDialectPrecisionAndScale(trimmedDataTypeDefinition, databaseType);
        return result.isPresent() ? result : extractStandardPrecisionAndScale(trimmedDataTypeDefinition);
    }
    
    private static Optional<Pair<Integer, Integer>> extractDialectPrecisionAndScale(final String dataTypeDefinition, final DatabaseType databaseType) {
        return DatabaseTypedSPILoader.findService(DialectDataTypeExtractor.class, databaseType).flatMap(optional -> optional.extractPrecisionAndScale(dataTypeDefinition));
    }
    
    private static Optional<Pair<Integer, Integer>> extractStandardPrecisionAndScale(final String dataTypeDefinition) {
        Matcher matcher = DECIMAL_PATTERN.matcher(dataTypeDefinition);
        if (matcher.find()) {
            int precision = Integer.parseInt(matcher.group(1));
            int scale = Integer.parseInt(matcher.group(2));
            return Optional.of(Pair.of(precision, scale));
        }
        matcher = DECIMAL_WITHOUT_SCALE_PATTERN.matcher(dataTypeDefinition);
        if (matcher.find()) {
            int precision = Integer.parseInt(matcher.group(1));
            return Optional.of(Pair.of(precision, 0));
        }
        matcher = NUMERIC_PATTERN.matcher(dataTypeDefinition);
        if (matcher.find()) {
            int precision = Integer.parseInt(matcher.group(1));
            int scale = Integer.parseInt(matcher.group(2));
            return Optional.of(Pair.of(precision, scale));
        }
        matcher = NUMERIC_WITHOUT_SCALE_PATTERN.matcher(dataTypeDefinition);
        if (matcher.find()) {
            int precision = Integer.parseInt(matcher.group(1));
            return Optional.of(Pair.of(precision, 0));
        }
        return Optional.empty();
    }
    
    private static Optional<String> extractDialectDataTypeName(final String dataTypeDefinition, final DatabaseType databaseType) {
        return DatabaseTypedSPILoader.findService(DialectDataTypeExtractor.class, databaseType).flatMap(optional -> optional.extract(dataTypeDefinition));
    }
    
    private static String extractStandardDataTypeName(final String dataTypeDefinition) {
        if (dataTypeDefinition.contains("(")) {
            return dataTypeDefinition.substring(0, dataTypeDefinition.indexOf("("));
        }
        if (dataTypeDefinition.contains(" ")) {
            return dataTypeDefinition.substring(0, dataTypeDefinition.indexOf(" "));
        }
        return dataTypeDefinition;
    }
}
