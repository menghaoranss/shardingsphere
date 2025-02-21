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

package com.sphereex.dbplusengine.encrypt.rewrite.token.cryptographic.generator.select;

import lombok.RequiredArgsConstructor;
import lombok.Setter;
import org.apache.shardingsphere.encrypt.rule.EncryptRule;
import org.apache.shardingsphere.encrypt.rule.column.EncryptColumn;
import org.apache.shardingsphere.encrypt.rule.table.EncryptTable;
import org.apache.shardingsphere.infra.binder.context.segment.select.projection.Projection;
import org.apache.shardingsphere.infra.binder.context.segment.select.projection.impl.ColumnProjection;
import org.apache.shardingsphere.infra.binder.context.statement.SQLStatementContext;
import org.apache.shardingsphere.infra.binder.context.statement.dml.SelectStatementContext;
import org.apache.shardingsphere.infra.database.core.metadata.database.enums.QuoteCharacter;
import org.apache.shardingsphere.infra.database.core.type.DatabaseType;
import org.apache.shardingsphere.infra.metadata.ShardingSphereMetaData;
import org.apache.shardingsphere.infra.metadata.database.ShardingSphereDatabase;
import org.apache.shardingsphere.infra.rewrite.sql.token.common.generator.CollectionSQLTokenGenerator;
import org.apache.shardingsphere.infra.rewrite.sql.token.common.pojo.SQLToken;
import org.apache.shardingsphere.infra.rewrite.sql.token.common.pojo.generic.SubstitutableColumnNameToken;
import org.apache.shardingsphere.sql.parser.statement.core.segment.dml.column.ColumnSegment;
import org.apache.shardingsphere.sql.parser.statement.core.value.identifier.IdentifierValue;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.Optional;

/**
 * Select for update token generator for encrypt.
 */
@RequiredArgsConstructor
@Setter
public final class EncryptSelectForUpdateTokenGenerator implements CollectionSQLTokenGenerator<SelectStatementContext> {
    
    private final EncryptRule rule;
    
    private final ShardingSphereDatabase database;
    
    private final ShardingSphereMetaData metaData;
    
    @Override
    public boolean isGenerateSQLToken(final SQLStatementContext sqlStatementContext) {
        return sqlStatementContext instanceof SelectStatementContext && (((SelectStatementContext) sqlStatementContext).getSqlStatement()).getLock().isPresent();
    }
    
    @SuppressWarnings("OptionalGetWithoutIsPresent")
    @Override
    public Collection<SQLToken> generateSQLTokens(final SelectStatementContext sqlStatementContext) {
        Collection<SQLToken> result = new LinkedList<>();
        for (ColumnSegment each : sqlStatementContext.getSqlStatement().getLock().get().getColumns()) {
            generateSQLTokens(each, sqlStatementContext.getDatabaseType()).ifPresent(result::add);
        }
        return result;
    }
    
    private Optional<SQLToken> generateSQLTokens(final ColumnSegment columnSegment, final DatabaseType databaseType) {
        String tableName = columnSegment.getColumnBoundInfo().getOriginalTable().getValue();
        String columnName = columnSegment.getColumnBoundInfo().getOriginalColumn().getValue();
        Optional<EncryptTable> encryptTable = rule.findEncryptTable(tableName);
        if (!encryptTable.isPresent() || !encryptTable.get().isEncryptColumn(columnName)) {
            return Optional.empty();
        }
        int startIndex = columnSegment.getOwner().isPresent() ? columnSegment.getOwner().get().getStopIndex() + 2 : columnSegment.getStartIndex();
        int stopIndex = columnSegment.getStopIndex();
        EncryptColumn encryptColumn = encryptTable.get().getEncryptColumn(columnName);
        QuoteCharacter columnQuoteCharacter = columnSegment.getIdentifier().getQuoteCharacter();
        if (encryptColumn.getPlain().isPresent() && rule.isQueryWithPlain(tableName, columnName)) {
            return Optional.of(new SubstitutableColumnNameToken(
                    startIndex, stopIndex, createColumnProjections(encryptColumn.getPlain().get().getName(), columnQuoteCharacter, databaseType), databaseType, database, metaData));
        }
        if (encryptColumn.getAssistedQuery().isPresent()) {
            return Optional.of(new SubstitutableColumnNameToken(
                    startIndex, stopIndex, createColumnProjections(encryptColumn.getAssistedQuery().get().getName(), columnQuoteCharacter, databaseType), databaseType, database, metaData));
        }
        return Optional.of(new SubstitutableColumnNameToken(startIndex, stopIndex, createColumnProjections(encryptColumn.getCipher().getName(), columnQuoteCharacter, databaseType), databaseType,
                database, metaData));
    }
    
    private Collection<Projection> createColumnProjections(final String columnName, final QuoteCharacter quoteCharacter, final DatabaseType databaseType) {
        return Collections.singleton(new ColumnProjection(null, new IdentifierValue(columnName, quoteCharacter), null, databaseType));
    }
}
