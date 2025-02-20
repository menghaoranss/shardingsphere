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

package com.sphereex.dbplusengine.infra.rewrite.util;

import com.google.common.base.Strings;
import com.sphereex.dbplusengine.infra.database.core.metadata.database.character.DialectCharacterLengthCalculator;
import com.sphereex.dbplusengine.infra.metadata.database.schema.model.props.SchemaPropertyKey;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.apache.shardingsphere.infra.binder.context.statement.ddl.AlterTableStatementContext;
import org.apache.shardingsphere.infra.binder.context.statement.ddl.CreateTableStatementContext;
import org.apache.shardingsphere.infra.config.props.ConfigurationProperties;
import org.apache.shardingsphere.infra.config.props.ConfigurationPropertyKey;
import org.apache.shardingsphere.infra.database.core.spi.DatabaseTypedSPILoader;
import org.apache.shardingsphere.infra.metadata.database.schema.model.ShardingSphereColumn;
import org.apache.shardingsphere.infra.metadata.database.schema.model.ShardingSphereSchema;
import org.apache.shardingsphere.infra.metadata.database.schema.model.ShardingSphereTable;
import org.apache.shardingsphere.sql.parser.statement.core.segment.ddl.column.ColumnDefinitionSegment;
import org.apache.shardingsphere.sql.parser.statement.core.segment.ddl.table.CreateTableOptionSegment;

import java.util.Optional;

/**
 * Column charset utility class.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class ColumnCharsetUtils {
    
    /**
     * Get column byte length.
     *
     * @param createTableStatementContext create table statement context
     * @param columnDefinition column definition
     * @param schema schema
     * @param props configuration properties
     * @return column byte length
     */
    public static String getColumnCharsetName(final CreateTableStatementContext createTableStatementContext, final ColumnDefinitionSegment columnDefinition, final ShardingSphereSchema schema,
                                              final ConfigurationProperties props) {
        if (columnDefinition.getCharsetName().isPresent()) {
            return columnDefinition.getCharsetName().get();
        }
        if (columnDefinition.getCollateName().isPresent()) {
            return DatabaseTypedSPILoader.getService(DialectCharacterLengthCalculator.class, createTableStatementContext.getDatabaseType())
                    .getCharsetNameByCollation(columnDefinition.getCollateName().get());
        }
        Optional<CreateTableOptionSegment> createTableOption = createTableStatementContext.getSqlStatement().getCreateTableOption();
        Optional<String> tableCharsetName = createTableOption.flatMap(CreateTableOptionSegment::getCharsetName);
        if (tableCharsetName.isPresent()) {
            return tableCharsetName.get();
        }
        Optional<String> tableCollateName = createTableOption.flatMap(CreateTableOptionSegment::getCollateName);
        if (tableCollateName.isPresent()) {
            return DatabaseTypedSPILoader.getService(DialectCharacterLengthCalculator.class, createTableStatementContext.getDatabaseType()).getCharsetNameByCollation(tableCollateName.get());
        }
        if (!Strings.isNullOrEmpty(schema.getProps().getValue(SchemaPropertyKey.CHARACTER_SET_NAME))) {
            return schema.getProps().getValue(SchemaPropertyKey.CHARACTER_SET_NAME);
        }
        if (!ConfigurationPropertyKey.DEFAULT_CHARACTER_SET_NAME.getDefaultValue().equalsIgnoreCase(props.getValue(ConfigurationPropertyKey.DEFAULT_CHARACTER_SET_NAME))) {
            return props.getValue(ConfigurationPropertyKey.DEFAULT_CHARACTER_SET_NAME);
        }
        return DatabaseTypedSPILoader.getService(DialectCharacterLengthCalculator.class, createTableStatementContext.getDatabaseType()).getDefaultCharsetName();
    }
    
    /**
     * Get column byte length.
     *
     * @param alterTableStatementContext alter table statement context
     * @param columnDefinition column definition
     * @param schema schema
     * @param props configuration properties
     * @return column byte length
     */
    public static String getColumnCharsetName(final AlterTableStatementContext alterTableStatementContext, final ColumnDefinitionSegment columnDefinition, final ShardingSphereSchema schema,
                                              final ConfigurationProperties props) {
        if (columnDefinition.getCharsetName().isPresent()) {
            return columnDefinition.getCharsetName().get();
        }
        if (columnDefinition.getCollateName().isPresent()) {
            return DatabaseTypedSPILoader.getService(DialectCharacterLengthCalculator.class, alterTableStatementContext.getDatabaseType())
                    .getCharsetNameByCollation(columnDefinition.getCollateName().get());
        }
        String tableName = alterTableStatementContext.getSqlStatement().getTable().getTableName().getIdentifier().getValue();
        String columnName = columnDefinition.getColumnName().getIdentifier().getValue();
        Optional<String> columnCharsetName = Optional.ofNullable(schema.getTable(tableName)).map(optional -> optional.getColumn(columnName)).map(ShardingSphereColumn::getCharacterSetName);
        if (columnCharsetName.isPresent()) {
            return columnCharsetName.get();
        }
        Optional<String> tableCharsetName = Optional.ofNullable(schema.getTable(tableName)).map(ShardingSphereTable::getCharacterSetName);
        return tableCharsetName
                .orElseGet(() -> Strings.isNullOrEmpty(schema.getProps().getValue(SchemaPropertyKey.CHARACTER_SET_NAME)) ? props.getValue(ConfigurationPropertyKey.DEFAULT_CHARACTER_SET_NAME)
                        : schema.getProps().getValue(SchemaPropertyKey.CHARACTER_SET_NAME));
    }
}
