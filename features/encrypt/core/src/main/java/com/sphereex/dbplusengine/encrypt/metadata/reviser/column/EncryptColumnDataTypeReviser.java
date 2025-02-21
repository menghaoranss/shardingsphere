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

package com.sphereex.dbplusengine.encrypt.metadata.reviser.column;

import com.sphereex.dbplusengine.encrypt.exception.metadata.EncryptDataTypeNotFoundException;
import com.sphereex.dbplusengine.infra.database.core.metadata.database.datatype.DataTypeExtractor;
import com.sphereex.dbplusengine.infra.metadata.database.schema.reviser.column.ColumnDataTypeReviser;
import lombok.RequiredArgsConstructor;
import org.apache.shardingsphere.encrypt.rule.table.EncryptTable;
import org.apache.shardingsphere.infra.database.core.metadata.data.model.ColumnMetaData;
import org.apache.shardingsphere.infra.database.core.metadata.database.datatype.DataTypeRegistry;
import org.apache.shardingsphere.infra.database.core.type.DatabaseType;
import org.apache.shardingsphere.infra.exception.core.ShardingSpherePreconditions;
import org.apache.shardingsphere.infra.exception.kernel.metadata.RuleAndStorageMetaDataMismatchedException;

import javax.sql.DataSource;
import java.util.Collection;
import java.util.Optional;

/**
 * Encrypt column data type reviser.
 */
@RequiredArgsConstructor
public final class EncryptColumnDataTypeReviser implements ColumnDataTypeReviser {
    
    private final EncryptTable encryptTable;
    
    @Override
    public Optional<Integer> revise(final String originalName, final DatabaseType databaseType, final DataSource dataSource) {
        return getDataTypeDefinition(originalName).flatMap(optional -> getDataType(optional, databaseType, dataSource));
    }
    
    @Override
    public Optional<String> revise(final String originalName, final Collection<ColumnMetaData> originalMetaDataList) {
        if (encryptTable.isCipherColumn(originalName)) {
            return originalMetaDataList.stream().filter(each -> each.getName().equals(encryptTable.getLogicColumnByCipherColumn(originalName))).findFirst().map(ColumnMetaData::getDataTypeContent);
        }
        if (encryptTable.isPlainColumn(originalName)) {
            return originalMetaDataList.stream().filter(each -> each.getName().equals(encryptTable.getLogicColumnByPlainColumn(originalName))).findFirst().map(ColumnMetaData::getDataTypeContent);
        }
        return Optional.empty();
    }
    
    private Optional<String> getDataTypeDefinition(final String originalName) {
        if (encryptTable.isCipherColumn(originalName)) {
            return Optional.ofNullable(encryptTable.getEncryptColumn(encryptTable.getLogicColumnByCipherColumn(originalName)).getDataType());
        }
        if (encryptTable.isPlainColumn(originalName)) {
            return Optional.ofNullable(encryptTable.getEncryptColumn(encryptTable.getLogicColumnByPlainColumn(originalName)).getDataType());
        }
        return Optional.empty();
    }
    
    private Optional<Integer> getDataType(final String dataTypeDefinition, final DatabaseType databaseType, final DataSource dataSource) {
        String dataTypeName = DataTypeExtractor.extract(dataTypeDefinition, databaseType);
        Optional<Integer> dataType = DataTypeRegistry.getDataType(databaseType.getType(), dataTypeName);
        if (dataType.isPresent()) {
            return dataType;
        }
        DataTypeRegistry.load(dataSource, databaseType.getType());
        return DataTypeRegistry.getDataType(databaseType.getType(), dataTypeName);
    }
    
    @Override
    public void check(final ColumnMetaData columnMetaData, final DatabaseType databaseType, final DataSource dataSource) {
        Optional<String> configDataTypeName = encryptTable.findDataTypeNameByActualColumnName(columnMetaData.getName());
        if (!configDataTypeName.isPresent()) {
            return;
        }
        int configDataType = getDataType(configDataTypeName.get(), databaseType, dataSource)
                .orElseThrow(() -> new EncryptDataTypeNotFoundException(columnMetaData.getName(), configDataTypeName.get()));
        ShardingSpherePreconditions.checkState(configDataType == columnMetaData.getDataType(), () -> new RuleAndStorageMetaDataMismatchedException(
                String.format("Data type '%s' of column '%s' is different with storage resource data type.", configDataTypeName.get(), columnMetaData.getName())));
    }
}
