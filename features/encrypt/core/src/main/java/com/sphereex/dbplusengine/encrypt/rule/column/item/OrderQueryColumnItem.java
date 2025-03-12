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

package com.sphereex.dbplusengine.encrypt.rule.column.item;

import com.sphereex.dbplusengine.encrypt.context.EncryptContext;
import com.sphereex.dbplusengine.encrypt.context.EncryptContextBuilder;
import com.sphereex.dbplusengine.infra.hint.EncryptColumnItemType;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import org.apache.shardingsphere.encrypt.rule.column.EncryptColumn;
import org.apache.shardingsphere.encrypt.rule.column.item.CipherColumnItem;
import org.apache.shardingsphere.encrypt.spi.EncryptAlgorithm;
import org.apache.shardingsphere.infra.algorithm.core.context.AlgorithmSQLContext;
import org.apache.shardingsphere.infra.database.core.type.DatabaseType;

import java.util.LinkedList;
import java.util.List;
import java.util.Optional;

/**
 * Like query column item.
 */
// TODO delete this class because it's same with LikeQueryColumnItem.
@RequiredArgsConstructor
public final class OrderQueryColumnItem {
    
    @Getter
    private final String name;
    
    @Getter
    private final EncryptAlgorithm encryptor;
    
    @Setter
    private String dataType;
    
    @Setter
    private EncryptColumn encryptColumn;
    
    @Setter
    private DatabaseType databaseType;
    
    /**
     * Get encrypt like query value.
     *
     * @param databaseName database name
     * @param schemaName schema name
     * @param tableName table name
     * @param logicColumnName logic column name
     * @param originalValue original value
     * @return like query values
     */
    public Object encrypt(final String databaseName, final String schemaName, final String tableName, final String logicColumnName, final Object originalValue) {
        if (null == originalValue) {
            return null;
        }
        return encryptor.encrypt(originalValue, new AlgorithmSQLContext(databaseName, schemaName, tableName, logicColumnName),
                EncryptContextBuilder.build(encryptColumn, databaseType, EncryptColumnItemType.ORDER_QUERY));
    }
    
    /**
     * Get encrypt like query values.
     *
     * @param databaseName database name
     * @param schemaName schema name
     * @param tableName table name
     * @param logicColumnName logic column name
     * @param originalValues original values
     * @return like query values
     */
    public List<Object> encrypt(final String databaseName, final String schemaName, final String tableName, final String logicColumnName, final List<Object> originalValues) {
        AlgorithmSQLContext algorithmSQLContext = new AlgorithmSQLContext(databaseName, schemaName, tableName, logicColumnName);
        EncryptContext encryptContext = EncryptContextBuilder.build(encryptColumn, databaseType, EncryptColumnItemType.ORDER_QUERY);
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
    public Object decrypt(final String databaseName, final String schemaName, final String tableName, final String logicColumnName, final Object cipherValue, final DatabaseType databaseType) {
        CipherColumnItem cipherColumnItem = new CipherColumnItem(name, encryptor);
        cipherColumnItem.setDataType(dataType);
        cipherColumnItem.setEncryptColumn(encryptColumn);
        cipherColumnItem.setDatabaseType(databaseType);
        return cipherColumnItem.decrypt(databaseName, schemaName, tableName, logicColumnName, cipherValue, databaseType);
    }
    
    /**
     * Get data type.
     *
     * @return data type
     */
    public Optional<String> getDataType() {
        return Optional.ofNullable(dataType);
    }
}
