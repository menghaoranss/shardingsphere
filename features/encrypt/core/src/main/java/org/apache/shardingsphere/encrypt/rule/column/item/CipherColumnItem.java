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

package org.apache.shardingsphere.encrypt.rule.column.item;

import com.sphereex.dbplusengine.SphereEx;
import com.sphereex.dbplusengine.SphereEx.Type;
import com.sphereex.dbplusengine.encrypt.context.EncryptColumnDataTypeContext;
import com.sphereex.dbplusengine.encrypt.context.EncryptContext;
import com.sphereex.dbplusengine.encrypt.context.EncryptContextBuilder;
import com.sphereex.dbplusengine.infra.database.core.metadata.database.datatype.DataValueConverter;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.SneakyThrows;
import org.apache.shardingsphere.encrypt.rule.column.EncryptColumn;
import org.apache.shardingsphere.encrypt.spi.EncryptAlgorithm;
import org.apache.shardingsphere.infra.algorithm.core.context.AlgorithmSQLContext;
import org.apache.shardingsphere.infra.database.core.type.DatabaseType;

import java.sql.Clob;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;

/**
 * Cipher column item.
 */
@RequiredArgsConstructor
@Getter
public final class CipherColumnItem {
    
    private final String name;
    
    private final EncryptAlgorithm encryptor;
    
    @Setter
    @SphereEx
    private String dataType;
    
    @Setter
    @SphereEx
    private EncryptColumn encryptColumn;
    
    @Setter
    @SphereEx
    private DatabaseType databaseType;
    
    /**
     * Encrypt.
     *
     * @param databaseName database name
     * @param schemaName schema name
     * @param tableName table name
     * @param logicColumnName logic column name
     * @param originalValue original value
     * @return encrypted value
     */
    public Object encrypt(final String databaseName, final String schemaName, final String tableName, final String logicColumnName, final Object originalValue) {
        if (null == originalValue) {
            return null;
        }
        return encryptor.encrypt(originalValue, new AlgorithmSQLContext(databaseName, schemaName, tableName, logicColumnName),
                // SPEX CHANGED: BEGIN
                EncryptContextBuilder.build(encryptColumn, databaseType));
        // SPEX CHANGED: END
    }
    
    /**
     * Encrypt.
     *
     * @param databaseName database name
     * @param schemaName schema name
     * @param tableName table name
     * @param logicColumnName logic column name
     * @param originalValues original values
     * @return encrypted values
     */
    public List<Object> encrypt(final String databaseName, final String schemaName, final String tableName, final String logicColumnName, final List<Object> originalValues) {
        AlgorithmSQLContext algorithmSQLContext = new AlgorithmSQLContext(databaseName, schemaName, tableName, logicColumnName);
        @SphereEx(Type.MODIFY)
        EncryptContext encryptContext = EncryptContextBuilder.build(encryptColumn, databaseType);
        List<Object> result = new LinkedList<>();
        for (Object each : originalValues) {
            result.add(null == each ? null : encryptor.encrypt(each, algorithmSQLContext, encryptContext));
        }
        return result;
    }
    
    /**
     * Decrypt.
     *
     * @param databaseName database name
     * @param schemaName schema name
     * @param tableName table name
     * @param logicColumnName logic column name
     * @param cipherValue cipher value
     * @param databaseType database type
     * @return decrypted value
     */
    public Object decrypt(final String databaseName, final String schemaName, final String tableName, final String logicColumnName, final Object cipherValue,
                          @SphereEx final DatabaseType databaseType) {
        if (null == cipherValue) {
            return null;
        }
        // SPEX CHANGED: BEGIN
        EncryptContext encryptContext = EncryptContextBuilder.build(encryptColumn, databaseType);
        // TODO if some algorithm need to handle original Clob, this should be changed
        Object convertedCipherValue = convertClobValueToString(cipherValue);
        return convertToOriginTypeIfNecessary(
                encryptor.decrypt(convertedCipherValue, new AlgorithmSQLContext(databaseName, schemaName, tableName, logicColumnName), encryptContext), encryptContext, databaseType);
        // SPEX CHANGED: END
    }
    
    @SphereEx
    @SneakyThrows(SQLException.class)
    private Object convertClobValueToString(final Object cipherValue) {
        if (cipherValue instanceof Clob) {
            return ((Clob) cipherValue).getSubString(1, (int) ((Clob) cipherValue).length());
        }
        return cipherValue;
    }
    
    @SphereEx
    private Object convertToOriginTypeIfNecessary(final Object decryptedValue, final EncryptContext encryptContext, final DatabaseType databaseType) {
        if (!encryptor.getMetaData().isSupportDecrypt()) {
            return decryptedValue;
        }
        EncryptColumnDataTypeContext columnDataType = encryptContext.getColumnDataType();
        if (null != columnDataType) {
            String logicDataType = columnDataType.getLogicDataType();
            if (null != logicDataType) {
                return new DataValueConverter().convert(decryptedValue, logicDataType, databaseType);
            }
        }
        return decryptedValue;
    }
    
    /**
     * Get data type.
     *
     * @return data type
     */
    @SphereEx
    public Optional<String> getDataType() {
        return Optional.ofNullable(dataType);
    }
}
