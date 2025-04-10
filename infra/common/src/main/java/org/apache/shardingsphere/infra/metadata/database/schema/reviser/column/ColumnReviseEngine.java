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

package org.apache.shardingsphere.infra.metadata.database.schema.reviser.column;

import com.sphereex.dbplusengine.SphereEx;
import com.sphereex.dbplusengine.infra.metadata.database.schema.reviser.column.ColumnDataTypeReviser;
import lombok.RequiredArgsConstructor;
import org.apache.shardingsphere.infra.database.core.metadata.data.model.ColumnMetaData;
import org.apache.shardingsphere.infra.database.core.type.DatabaseType;
import org.apache.shardingsphere.infra.exception.core.ShardingSpherePreconditions;
import org.apache.shardingsphere.infra.metadata.database.schema.reviser.MetaDataReviseEntry;
import org.apache.shardingsphere.infra.rule.ShardingSphereRule;

import javax.sql.DataSource;
import java.util.Collection;
import java.util.LinkedList;
import java.util.Optional;

/**
 * Column revise engine.
 *
 * @param <T> type of rule
 */
@RequiredArgsConstructor
public final class ColumnReviseEngine<T extends ShardingSphereRule> {
    
    private final T rule;
    
    private final MetaDataReviseEntry<T> reviseEntry;
    
    @SphereEx
    private final DatabaseType databaseType;
    
    @SphereEx
    private final DataSource dataSource;
    
    /**
     * Revise column meta data.
     *
     * @param tableName table name
     * @param originalMetaDataList original column meta data list
     * @return revised column meta data
     */
    public Collection<ColumnMetaData> revise(final String tableName, final Collection<ColumnMetaData> originalMetaDataList) {
        Optional<? extends ColumnExistedReviser> existedReviser = reviseEntry.getColumnExistedReviser(rule, tableName);
        Optional<? extends ColumnNameReviser> nameReviser = reviseEntry.getColumnNameReviser(rule, tableName);
        // SPEX ADDED: BEGIN
        Optional<? extends ColumnDataTypeReviser> dataTypeReviser = reviseEntry.getColumnDataTypeReviser(rule, tableName);
        // SPEX ADDED: END
        Optional<? extends ColumnGeneratedReviser> generatedReviser = reviseEntry.getColumnGeneratedReviser(rule, tableName);
        Collection<ColumnMetaData> result = new LinkedList<>();
        for (ColumnMetaData each : originalMetaDataList) {
            if (existedReviser.isPresent() && !existedReviser.get().isExisted(each.getName())) {
                continue;
            }
            String name = nameReviser.isPresent() ? nameReviser.get().revise(each.getName()) : each.getName();
            // SPEX ADDED: BEGIN
            // checkRevisePrimaryKey(result, tableName, name);
            int dataType = dataTypeReviser.map(optional -> optional.revise(each.getName(), databaseType, dataSource).orElseGet(each::getDataType)).orElseGet(each::getDataType);
            // SPEX ADDED: END
            boolean generated = generatedReviser.map(optional -> optional.revise(each)).orElseGet(each::isGenerated);
            // SPEX CHANGED: BEGIN
            String dataTypeContent = dataTypeReviser.map(optional -> optional.revise(each.getName(), originalMetaDataList).orElseGet(each::getDataTypeContent)).orElseGet(each::getDataTypeContent);
            result.add(new ColumnMetaData(name, dataType, each.isPrimaryKey(), generated, each.isCaseSensitive(), each.isVisible(), each.isUnsigned(), each.isNullable(), dataTypeContent,
                    each.getCharacterSetName()));
            // SPEX CHANGED: END
        }
        return result;
    }
    
    @SphereEx
    private void checkRevisePrimaryKey(final Collection<ColumnMetaData> columns, final String tableName, final String revisedColumnName) {
        for (ColumnMetaData each : columns) {
            if (each.getName().equals(revisedColumnName)) {
                ShardingSpherePreconditions.checkState(!each.isPrimaryKey(),
                        () -> new UnsupportedOperationException(String.format("Encrypt primary key `%s` of table `%s` is not supported", revisedColumnName, tableName)));
            }
        }
    }
    
    private ColumnMetaData getColumnMetaDataWithoutDataTypeRevise(final Optional<? extends ColumnNameReviser> nameReviser,
                                                                  final Optional<? extends ColumnGeneratedReviser> generatedReviser, final ColumnMetaData originalMetaData) {
        String name = nameReviser.isPresent() ? nameReviser.get().revise(originalMetaData.getName()) : originalMetaData.getName();
        boolean generated = generatedReviser.map(optional -> optional.revise(originalMetaData)).orElseGet(originalMetaData::isGenerated);
        return new ColumnMetaData(name, originalMetaData.getDataType(), originalMetaData.isPrimaryKey(), generated,
                // SPEX CHANGED: BEGIN
                originalMetaData.isCaseSensitive(), originalMetaData.isVisible(), originalMetaData.isUnsigned(), originalMetaData.isNullable(), originalMetaData.getDataTypeContent(),
                originalMetaData.getCharacterSetName());
        // SPEX CHANGED: END
    }
}
