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

package com.sphereex.dbplusengine.infra.database.core.metadata.database.character;

import org.apache.shardingsphere.infra.database.core.spi.DatabaseTypedSPI;
import org.apache.shardingsphere.infra.spi.annotation.SingletonSPI;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Dialect character length calculator.
 */
@SingletonSPI
public interface DialectCharacterLengthCalculator extends DatabaseTypedSPI {
    
    /**
     * Whether need calculate column and row character length or not.
     *
     * @return whether need calculate or not
     */
    boolean isNeedCalculate();
    
    /**
     * Get charset char to byte ratio.
     *
     * @param charset charset
     * @return charset char to byte ratio
     */
    int getCharsetCharToByteRatio(String charset);
    
    /**
     * Get charset name by collation.
     *
     * @param collation collation
     * @return charset name
     */
    String getCharsetNameByCollation(String collation);
    
    /**
     * Judge whether type is character type or not.
     *
     * @param type type 
     * @return whether is character type or not
     */
    boolean isCharacterType(String type);
    
    /**
     * Get default charset name.
     *
     * @return default charset name
     */
    String getDefaultCharsetName();
    
    /**
     * Check column byte length.
     *
     * @param columnByteLength column byte length
     * @param columnName column name
     */
    void checkColumnByteLength(int columnByteLength, String columnName);
    
    /**
     * Check row byte length.
     *
     * @param columnByteLength column byte length
     */
    void checkRowByteLength(int columnByteLength);
    
    /**
     * Calculate column byte length by column char length.
     *
     * @param columnCharLength column char length
     * @param notNull not null
     * @param dataType data type
     * @param columnName column name
     * @param columnCharset column charset
     * @param isNullCalculated is null calculated
     * @return column byte length
     */
    int calculateColumnByteLength(int columnCharLength, boolean notNull, String dataType, String columnName, String columnCharset, AtomicBoolean isNullCalculated);
    
    /**
     * Convert byte length to character length.
     *
     * @param byteLength byte length
     * @param charset charset
     * @return character length
     */
    int toCharacterLength(int byteLength, String charset);
    
    /**
     * Whether supported column character set definition or not.
     *
     * @return whether supported column character set definition or not
     */
    boolean isSupportedColumnCharacterSetDefinition();
    
    /**
     * Get default column length unit type.
     *
     * @return default column length unit type
     */
    String getDefaultColumnLengthUnit();
}
