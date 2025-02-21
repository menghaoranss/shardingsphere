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

package com.sphereex.dbplusengine.infra.expansibility;

import com.google.common.base.Strings;
import com.sphereex.dbplusengine.infra.database.core.metadata.database.character.DialectCharacterLengthCalculator;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.apache.shardingsphere.infra.database.core.spi.DatabaseTypedSPILoader;
import org.apache.shardingsphere.infra.database.core.type.DatabaseType;
import org.apache.shardingsphere.sql.parser.statement.core.segment.ddl.column.ColumnDefinitionSegment;
import org.apache.shardingsphere.sql.parser.statement.core.segment.generic.DataTypeLengthSegment;
import org.apache.shardingsphere.sql.parser.statement.core.segment.generic.DataTypeSegment;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Expansibility calculator utility class.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class ExpansibilityCalculatorUtils {
    
    private static final int DEFAULT_DATA_LENGTH = 4000;
    
    public static final String DEFAULT_DATA_TYPE = String.format("VARCHAR(%s)", DEFAULT_DATA_LENGTH);
    
    /**
     * Calculate column data type.
     *
     * @param expansibility expansibility
     * @param column column
     * @param charsetName charset name
     * @param databaseType database type
     * @param isUseSpecifiedCharset is use specified charset
     * @return data type
     */
    public static String calculateColumnDataType(final ExpansibilityProvider expansibility, final ColumnDefinitionSegment column, final String charsetName, final DatabaseType databaseType,
                                                 final boolean isUseSpecifiedCharset) {
        String dataTypeName = column.getDataType().getDataTypeName();
        Optional<Integer> dataTypeLength = column.getDataType().getDataTypeLength().map(DataTypeLengthSegment::getPrecision);
        if (Strings.isNullOrEmpty(charsetName) || Strings.isNullOrEmpty(dataTypeName) || !dataTypeLength.isPresent()) {
            return DEFAULT_DATA_TYPE;
        }
        DialectCharacterLengthCalculator calculator = DatabaseTypedSPILoader.getService(DialectCharacterLengthCalculator.class, databaseType);
        if (!calculator.isNeedCalculate() || !calculator.isCharacterType(dataTypeName)) {
            return DEFAULT_DATA_TYPE;
        }
        int calculatedColumnCharLength = calculateColumnCharLength(expansibility, column, charsetName, calculator);
        return buildColumnDefinition(column, isUseSpecifiedCharset, dataTypeName, calculatedColumnCharLength, calculator);
    }
    
    /**
     * Calculate column byte length.
     *
     * @param column column
     * @param provider provider
     * @param actualColumnName actual column name
     * @param columnCharset column charset
     * @param calculator calculator
     * @param isNullCalculated is null calculated
     * @param customDataType custom data type
     * @return column byte length calculated.
     */
    public static int calculateColumnByteLength(final ColumnDefinitionSegment column, final ExpansibilityProvider provider, final String actualColumnName, final String columnCharset,
                                                final DialectCharacterLengthCalculator calculator, final AtomicBoolean isNullCalculated, final String customDataType) {
        if (!Strings.isNullOrEmpty(customDataType)) {
            return 0;
        }
        int columnCharLength = calculateColumnCharLength(provider, column, columnCharset, calculator);
        return calculateByteLengthByCharLength(columnCharLength, column, actualColumnName, columnCharset, calculator, isNullCalculated);
    }
    
    private static int calculateColumnCharLength(final ExpansibilityProvider expansibilityProvider, final ColumnDefinitionSegment column, final String charsetName,
                                                 final DialectCharacterLengthCalculator calculator) {
        String dataTypeName = column.getDataType().getDataTypeName();
        Optional<Integer> dataTypeLength = column.getDataType().getDataTypeLength().map(DataTypeLengthSegment::getPrecision);
        if (Strings.isNullOrEmpty(charsetName) || Strings.isNullOrEmpty(dataTypeName) || !dataTypeLength.isPresent()) {
            return DEFAULT_DATA_LENGTH;
        }
        if (!calculator.isCharacterType(dataTypeName)) {
            return DEFAULT_DATA_LENGTH;
        }
        return expansibilityProvider.calculate(getOriginalColumnCharLength(column, calculator, charsetName), calculator.getCharsetCharToByteRatio(charsetName));
    }
    
    private static String buildColumnDefinition(final ColumnDefinitionSegment column, final boolean isUseSpecifiedCharset, final String dataTypeName, final int calculatedColumnCharLength,
                                                final DialectCharacterLengthCalculator calculator) {
        StringBuilder result = new StringBuilder();
        result.append(dataTypeName).append("(").append(calculatedColumnCharLength).append(getDataLengthType(column.getDataType())).append(")");
        if (calculator.isSupportedColumnCharacterSetDefinition() && isUseSpecifiedCharset) {
            result.append(" CHARACTER SET ").append(calculator.getDefaultCharsetName());
        }
        if (column.isNotNull()) {
            result.append(" NOT NULL");
        }
        return result.toString();
    }
    
    /**
     * Get original column char length.
     *
     * @param column column
     * @param calculator calculator
     * @param columnCharset column charset
     * @return column char length
     */
    public static Integer getOriginalColumnCharLength(final ColumnDefinitionSegment column, final DialectCharacterLengthCalculator calculator, final String columnCharset) {
        Optional<DataTypeLengthSegment> dataTypeLength = column.getDataType().getDataTypeLength();
        if (!dataTypeLength.isPresent()) {
            return 0;
        }
        String columnLengthUnitType = dataTypeLength.get().getType().orElseGet(calculator::getDefaultColumnLengthUnit);
        if ("BYTE".equalsIgnoreCase(columnLengthUnitType)) {
            return calculator.toCharacterLength(dataTypeLength.map(DataTypeLengthSegment::getPrecision).orElse(0), columnCharset);
        }
        return dataTypeLength.map(DataTypeLengthSegment::getPrecision).orElse(0);
    }
    
    private static String getDataLengthType(final DataTypeSegment dataType) {
        return dataType.getDataLength().getType().map(optional -> " " + optional).orElse("");
    }
    
    private static int calculateByteLengthByCharLength(final int columnCharLength, final ColumnDefinitionSegment column, final String actualColumnName, final String columnCharset,
                                                       final DialectCharacterLengthCalculator calculator, final AtomicBoolean isNullCalculated) {
        return calculator.calculateColumnByteLength(columnCharLength, column.isNotNull(), column.getDataType().getDataTypeName(), actualColumnName, columnCharset, isNullCalculated);
    }
}
