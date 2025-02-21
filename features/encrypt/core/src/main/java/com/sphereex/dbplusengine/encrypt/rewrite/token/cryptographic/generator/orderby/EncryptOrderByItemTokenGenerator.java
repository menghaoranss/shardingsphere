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

package com.sphereex.dbplusengine.encrypt.rewrite.token.cryptographic.generator.orderby;

import com.sphereex.dbplusengine.encrypt.rewrite.token.cryptographic.util.EncryptTokenGeneratorUtils;
import com.sphereex.dbplusengine.encrypt.rule.column.item.OrderQueryColumnItem;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import org.apache.shardingsphere.encrypt.rule.EncryptRule;
import org.apache.shardingsphere.encrypt.rule.column.EncryptColumn;
import org.apache.shardingsphere.encrypt.rule.table.EncryptTable;
import org.apache.shardingsphere.infra.annotation.HighFrequencyInvocation;
import org.apache.shardingsphere.infra.binder.context.segment.select.orderby.OrderByItem;
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
import org.apache.shardingsphere.sql.parser.statement.core.segment.dml.order.item.ColumnOrderByItemSegment;
import org.apache.shardingsphere.sql.parser.statement.core.segment.generic.OwnerSegment;
import org.apache.shardingsphere.sql.parser.statement.core.value.identifier.IdentifierValue;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.Optional;

/**
 * Order by item token generator for encrypt.
 */
@HighFrequencyInvocation
@RequiredArgsConstructor
@Setter
public final class EncryptOrderByItemTokenGenerator implements CollectionSQLTokenGenerator<SelectStatementContext> {
    
    private final EncryptRule rule;
    
    private final ShardingSphereDatabase database;
    
    private final ShardingSphereMetaData metaData;
    
    @Override
    public boolean isGenerateSQLToken(final SQLStatementContext sqlStatementContext) {
        return sqlStatementContext instanceof SelectStatementContext && containsOrderByItem((SelectStatementContext) sqlStatementContext);
    }
    
    private boolean containsOrderByItem(final SelectStatementContext sqlStatementContext) {
        if (!sqlStatementContext.getOrderByContext().getItems().isEmpty() && !sqlStatementContext.getOrderByContext().isGenerated()) {
            return true;
        }
        for (SelectStatementContext each : sqlStatementContext.getSubqueryContexts().values()) {
            if (containsOrderByItem(each)) {
                return true;
            }
        }
        return false;
    }
    
    @Override
    public Collection<SQLToken> generateSQLTokens(final SelectStatementContext sqlStatementContext) {
        Collection<SQLToken> result = new LinkedList<>();
        for (OrderByItem each : getOrderByItems(sqlStatementContext)) {
            if (each.getSegment() instanceof ColumnOrderByItemSegment) {
                ColumnSegment columnSegment = ((ColumnOrderByItemSegment) each.getSegment()).getColumn();
                result.addAll(generateSQLTokens(Collections.singleton(columnSegment), sqlStatementContext));
            }
        }
        return result;
    }
    
    private Collection<SubstitutableColumnNameToken> generateSQLTokens(final Collection<ColumnSegment> columnSegments, final SelectStatementContext sqlStatementContext) {
        Collection<SubstitutableColumnNameToken> result = new LinkedList<>();
        for (ColumnSegment each : columnSegments) {
            String tableName = each.getColumnBoundInfo().getOriginalTable().getValue();
            Optional<EncryptTable> encryptTable = rule.findEncryptTable(tableName);
            String columnName = each.getColumnBoundInfo().getOriginalColumn().getValue();
            if (!encryptTable.isPresent() || !encryptTable.get().isEncryptColumn(columnName)) {
                continue;
            }
            int startIndex = each.getOwner().isPresent() ? each.getOwner().get().getStartIndex() : each.getStartIndex();
            int stopIndex = each.getStopIndex();
            EncryptColumn encryptColumn = encryptTable.get().getEncryptColumn(columnName);
            String orderByColumnName = getOrderByColumnName(encryptColumn);
            boolean encryptColumnContainsInGroupByItem = EncryptTokenGeneratorUtils.isEncryptColumnContainsInGroupByItem(each, encryptColumn, sqlStatementContext);
            DatabaseType databaseType = sqlStatementContext.getDatabaseType();
            SubstitutableColumnNameToken encryptColumnNameToken =
                    new SubstitutableColumnNameToken(startIndex, stopIndex, createColumnProjections(each.getOwner().map(OwnerSegment::getIdentifier).orElse(null), orderByColumnName,
                            each.getIdentifier().getQuoteCharacter(), databaseType, encryptColumnContainsInGroupByItem), databaseType, database, metaData);
            result.add(encryptColumnNameToken);
        }
        return result;
    }
    
    private String getOrderByColumnName(final EncryptColumn encryptColumn) {
        if (encryptColumn.getPlain().isPresent() && encryptColumn.getPlain().get().isQueryWithPlain()) {
            return encryptColumn.getPlain().get().getName();
        }
        return encryptColumn.getOrderQuery().map(OrderQueryColumnItem::getName).orElseGet(() -> encryptColumn.getCipher().getName());
    }
    
    private Collection<OrderByItem> getOrderByItems(final SelectStatementContext sqlStatementContext) {
        Collection<OrderByItem> result = new LinkedList<>();
        if (!sqlStatementContext.getOrderByContext().isGenerated()) {
            result.addAll(sqlStatementContext.getOrderByContext().getItems());
        }
        for (SelectStatementContext each : sqlStatementContext.getSubqueryContexts().values()) {
            result.addAll(getOrderByItems(each));
        }
        return result;
    }
    
    private Collection<Projection> createColumnProjections(final IdentifierValue owner, final String columnName, final QuoteCharacter quoteCharacter, final DatabaseType databaseType,
                                                           final boolean encryptColumnContainsInGroupByItem) {
        ColumnProjection columnProjection = new ColumnProjection(owner, new IdentifierValue(columnName, quoteCharacter), null, databaseType);
        columnProjection.setEncryptColumnContainsInGroupByItem(encryptColumnContainsInGroupByItem);
        return Collections.singleton(columnProjection);
    }
}
