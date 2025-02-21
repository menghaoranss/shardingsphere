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

package com.sphereex.dbplusengine.encrypt.rewrite.util;

import com.sphereex.dbplusengine.encrypt.rule.column.item.OrderQueryColumnItem;
import com.sphereex.dbplusengine.infra.database.core.metadata.database.character.DialectCharacterLengthCalculator;
import com.sphereex.dbplusengine.infra.expansibility.ExpansibilityCalculatorUtils;
import com.sphereex.dbplusengine.infra.rewrite.util.ColumnCharsetUtils;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.apache.shardingsphere.encrypt.constant.EncryptColumnDataType;
import org.apache.shardingsphere.encrypt.rule.column.EncryptColumn;
import org.apache.shardingsphere.encrypt.rule.column.item.AssistedQueryColumnItem;
import org.apache.shardingsphere.encrypt.rule.column.item.CipherColumnItem;
import org.apache.shardingsphere.encrypt.rule.column.item.LikeQueryColumnItem;
import org.apache.shardingsphere.encrypt.rule.table.EncryptTable;
import org.apache.shardingsphere.infra.binder.context.statement.ddl.CreateTableStatementContext;
import org.apache.shardingsphere.infra.config.props.ConfigurationProperties;
import org.apache.shardingsphere.infra.database.core.spi.DatabaseTypedSPILoader;
import org.apache.shardingsphere.infra.database.core.type.DatabaseType;
import org.apache.shardingsphere.infra.metadata.database.schema.model.ShardingSphereSchema;
import org.apache.shardingsphere.sql.parser.statement.core.segment.ddl.column.ColumnDefinitionSegment;
import org.apache.shardingsphere.sql.parser.statement.core.segment.generic.DataTypeLengthSegment;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Encrypt data type utility class.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class EncryptDataTypeUtils {
    
    /**
     * Get data type of cipher column.
     *
     * @param cipherColumnItem cipher column item
     * @return data type
     */
    public static String getDataType(final CipherColumnItem cipherColumnItem) {
        return cipherColumnItem.getDataType().isPresent() ? cipherColumnItem.getDataType().get() : EncryptColumnDataType.DEFAULT_DATA_TYPE;
    }
    
    /**
     * Get data type of assisted query column.
     *
     * @param assistedQueryColumnItem assisted query column item
     * @return data type
     */
    public static String getDataType(final AssistedQueryColumnItem assistedQueryColumnItem) {
        return assistedQueryColumnItem.getDataType().isPresent() ? assistedQueryColumnItem.getDataType().get() : EncryptColumnDataType.DEFAULT_DATA_TYPE;
    }
    
    /**
     * Get data type of like query column.
     *
     * @param likeQueryColumnItem like query column item
     * @return data type
     */
    public static String getDataType(final LikeQueryColumnItem likeQueryColumnItem) {
        return likeQueryColumnItem.getDataType().isPresent() ? likeQueryColumnItem.getDataType().get() : EncryptColumnDataType.DEFAULT_DATA_TYPE;
    }
    
    /**
     * Get data type of order query column.
     *
     * @param orderQueryColumnItem order query column item
     * @return data type
     */
    public static String getDataType(final OrderQueryColumnItem orderQueryColumnItem) {
        return orderQueryColumnItem.getDataType().isPresent() ? orderQueryColumnItem.getDataType().get() : EncryptColumnDataType.DEFAULT_DATA_TYPE;
    }
    
    /**
     * Get data type of cipher column.
     *
     * @param cipherColumnItem cipher column item
     * @param column column
     * @param charsetName charset name
     * @param databaseType database type
     * @return data type
     */
    public static String getDataType(final CipherColumnItem cipherColumnItem, final ColumnDefinitionSegment column, final String charsetName, final DatabaseType databaseType) {
        return cipherColumnItem.getDataType().isPresent() ? cipherColumnItem.getDataType().get()
                : ExpansibilityCalculatorUtils.calculateColumnDataType(cipherColumnItem.getEncryptor().getMetaData().getExpansibility(), column, charsetName, databaseType, false);
    }
    
    /**
     * Get data type of assisted query column.
     *
     * @param assistedQueryColumnItem assisted query column item
     * @param column column
     * @param charsetName charset name
     * @param databaseType database type
     * @return data type
     */
    public static String getDataType(final AssistedQueryColumnItem assistedQueryColumnItem, final ColumnDefinitionSegment column, final String charsetName, final DatabaseType databaseType) {
        return assistedQueryColumnItem.getDataType().isPresent() ? assistedQueryColumnItem.getDataType().get()
                : ExpansibilityCalculatorUtils.calculateColumnDataType(assistedQueryColumnItem.getEncryptor().getMetaData().getExpansibility(), column, charsetName, databaseType, false);
    }
    
    /**
     * Get data type of like query column.
     *
     * @param likeQueryColumnItem like query column item
     * @param column column
     * @param charsetName charset name
     * @param databaseType database type
     * @param isUseSpecifiedCharset whether use specified charset or not
     * @return data type
     */
    public static String getDataType(final LikeQueryColumnItem likeQueryColumnItem, final ColumnDefinitionSegment column, final String charsetName, final DatabaseType databaseType,
                                     final boolean isUseSpecifiedCharset) {
        return likeQueryColumnItem.getDataType().isPresent() ? likeQueryColumnItem.getDataType().get()
                : ExpansibilityCalculatorUtils.calculateColumnDataType(likeQueryColumnItem.getEncryptor().getMetaData().getExpansibility(), column, charsetName, databaseType, isUseSpecifiedCharset);
    }
    
    /**
     * Get data type of order query column.
     *
     * @param orderQueryColumnItem order query column item
     * @param column column
     * @param charsetName charset name
     * @param databaseType database type
     * @return data type
     */
    public static String getDataType(final OrderQueryColumnItem orderQueryColumnItem, final ColumnDefinitionSegment column, final String charsetName, final DatabaseType databaseType) {
        return orderQueryColumnItem.getDataType().isPresent() ? orderQueryColumnItem.getDataType().get()
                : ExpansibilityCalculatorUtils.calculateColumnDataType(orderQueryColumnItem.getEncryptor().getMetaData().getExpansibility(), column, charsetName, databaseType, false);
    }
    
    /**
     * Check row byte length.
     *
     * @param createTableStatementContext create table statement context
     * @param columns columns
     * @param databaseType database type
     * @param encryptTable encrypt table
     * @param schema schema
     * @param props configuration properties
     */
    public static void checkRowByteLength(final CreateTableStatementContext createTableStatementContext, final List<ColumnDefinitionSegment> columns, final DatabaseType databaseType,
                                          final EncryptTable encryptTable, final ShardingSphereSchema schema, final ConfigurationProperties props) {
        int totalByteLength = 0;
        AtomicBoolean isNullCalculated = new AtomicBoolean(false);
        DialectCharacterLengthCalculator calculator = DatabaseTypedSPILoader.getService(DialectCharacterLengthCalculator.class, databaseType);
        if (!calculator.isNeedCalculate()) {
            return;
        }
        for (ColumnDefinitionSegment each : columns) {
            totalByteLength += calculateColumnByteLength(createTableStatementContext, each, encryptTable, calculator, schema, props, isNullCalculated);
        }
        calculator.checkRowByteLength(totalByteLength);
    }
    
    private static int calculateColumnByteLength(final CreateTableStatementContext createTableStatementContext, final ColumnDefinitionSegment column, final EncryptTable encryptTable,
                                                 final DialectCharacterLengthCalculator calculator, final ShardingSphereSchema schema, final ConfigurationProperties props,
                                                 final AtomicBoolean isNullCalculated) {
        String columnCharset = ColumnCharsetUtils.getColumnCharsetName(createTableStatementContext, column, schema, props);
        String columnName = column.getColumnName().getIdentifier().getValue();
        if (calculator.isCharacterType(column.getDataType().getDataTypeName())) {
            if (encryptTable.isEncryptColumn(columnName)) {
                EncryptColumn encryptColumn = encryptTable.getEncryptColumn(columnName);
                int cipherByteLength = ExpansibilityCalculatorUtils.calculateColumnByteLength(column, encryptColumn.getCipher().getEncryptor().getMetaData().getExpansibility(),
                        encryptColumn.getCipher().getName(), columnCharset, calculator, isNullCalculated, encryptColumn.getCipher().getDataType().orElse(""));
                int assistedQueryByteLength =
                        encryptColumn.getAssistedQuery()
                                .map(optional -> ExpansibilityCalculatorUtils.calculateColumnByteLength(column, optional.getEncryptor().getMetaData().getExpansibility(), optional.getName(),
                                        columnCharset, calculator, isNullCalculated, optional.getDataType().orElse("")))
                                .orElse(0);
                int likeQueryByteLength =
                        encryptColumn.getLikeQuery()
                                .map(optional -> ExpansibilityCalculatorUtils.calculateColumnByteLength(column, optional.getEncryptor().getMetaData().getExpansibility(), optional.getName(),
                                        columnCharset, calculator, isNullCalculated, optional.getDataType().orElse("")))
                                .orElse(0);
                int orderQueryByteLength =
                        encryptColumn.getOrderQuery()
                                .map(optional -> ExpansibilityCalculatorUtils.calculateColumnByteLength(column, optional.getEncryptor().getMetaData().getExpansibility(), optional.getName(),
                                        columnCharset, calculator, isNullCalculated, optional.getDataType().orElse("")))
                                .orElse(0);
                int plainByteLength =
                        calculateByteLengthByCharLength(encryptColumn.getPlain().map(optional -> column.getDataType().getDataTypeLength().map(DataTypeLengthSegment::getPrecision).orElse(0)).orElse(0),
                                column, columnName, columnCharset, calculator, isNullCalculated);
                return cipherByteLength + assistedQueryByteLength + likeQueryByteLength + orderQueryByteLength + plainByteLength;
            }
            int columnCharLength = ExpansibilityCalculatorUtils.getOriginalColumnCharLength(column, calculator, columnCharset);
            return calculateByteLengthByCharLength(columnCharLength, column, columnName, columnCharset, calculator, isNullCalculated);
        }
        return calculateByteLengthByCharLength(0, column, columnName, columnCharset, calculator, isNullCalculated);
    }
    
    private static int calculateByteLengthByCharLength(final int columnCharLength, final ColumnDefinitionSegment column, final String actualColumnName, final String columnCharset,
                                                       final DialectCharacterLengthCalculator calculator, final AtomicBoolean isNullCalculated) {
        return calculator.calculateColumnByteLength(columnCharLength, column.isNotNull(), column.getDataType().getDataTypeName(), actualColumnName, columnCharset, isNullCalculated);
    }
}
