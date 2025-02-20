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

package com.sphereex.dbplusengine.encrypt.rewrite.token.job.generator.select;

import com.sphereex.dbplusengine.encrypt.rewrite.aware.HintValueContextAware;
import com.sphereex.dbplusengine.encrypt.rewrite.token.job.pojo.select.NoneUniqueKeyScenarioAddColumnToken;
import com.sphereex.dbplusengine.encrypt.rule.column.item.OrderQueryColumnItem;
import com.sphereex.dbplusengine.encrypt.rule.column.item.PlainColumnItem;
import com.sphereex.dbplusengine.infra.hint.EncryptColumnItemType;
import com.sphereex.dbplusengine.infra.hint.NoneUniqueKeyScenario;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import org.apache.shardingsphere.encrypt.rule.EncryptRule;
import org.apache.shardingsphere.encrypt.rule.column.EncryptColumn;
import org.apache.shardingsphere.encrypt.rule.column.item.AssistedQueryColumnItem;
import org.apache.shardingsphere.encrypt.rule.column.item.LikeQueryColumnItem;
import org.apache.shardingsphere.encrypt.rule.table.EncryptTable;
import org.apache.shardingsphere.infra.annotation.HighFrequencyInvocation;
import org.apache.shardingsphere.infra.binder.context.segment.select.projection.Projection;
import org.apache.shardingsphere.infra.binder.context.segment.select.projection.impl.ColumnProjection;
import org.apache.shardingsphere.infra.binder.context.statement.SQLStatementContext;
import org.apache.shardingsphere.infra.binder.context.statement.dml.SelectStatementContext;
import org.apache.shardingsphere.infra.database.core.metadata.database.enums.QuoteCharacter;
import org.apache.shardingsphere.infra.hint.HintValueContext;
import org.apache.shardingsphere.infra.metadata.ShardingSphereMetaData;
import org.apache.shardingsphere.infra.metadata.database.ShardingSphereDatabase;
import org.apache.shardingsphere.infra.rewrite.sql.token.common.generator.CollectionSQLTokenGenerator;
import org.apache.shardingsphere.infra.rewrite.sql.token.common.pojo.SQLToken;
import org.apache.shardingsphere.infra.rewrite.sql.token.common.pojo.generic.SubstitutableColumnNameToken;
import org.apache.shardingsphere.sql.parser.statement.core.segment.dml.column.ColumnSegment;
import org.apache.shardingsphere.sql.parser.statement.core.segment.dml.item.ColumnProjectionSegment;
import org.apache.shardingsphere.sql.parser.statement.core.segment.dml.item.ProjectionSegment;
import org.apache.shardingsphere.sql.parser.statement.core.value.identifier.IdentifierValue;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.Optional;

/**
 * Select token generator for encrypt.
 */
@HighFrequencyInvocation
@RequiredArgsConstructor
@Setter
public final class EncryptSelectTokenGenerator implements CollectionSQLTokenGenerator<SelectStatementContext>, HintValueContextAware {
    
    private final EncryptRule rule;
    
    private final ShardingSphereMetaData metaData;
    
    private final ShardingSphereDatabase database;
    
    private HintValueContext hintValueContext;
    
    @Override
    public boolean isGenerateSQLToken(final SQLStatementContext sqlStatementContext) {
        return sqlStatementContext instanceof SelectStatementContext && !((SelectStatementContext) sqlStatementContext).getTablesContext().getSimpleTables().isEmpty()
                && NoneUniqueKeyScenario.ENCRYPT == hintValueContext.getNoneUniqueKeyScenario();
    }
    
    @Override
    public Collection<SQLToken> generateSQLTokens(final SelectStatementContext sqlStatementContext) {
        EncryptTable table = rule.getEncryptTable(sqlStatementContext.getTablesContext().getTableNames().stream().findFirst().orElse(null));
        Collection<SQLToken> result = generateProjectionSQLTokens(sqlStatementContext, table);
        result.addAll(generateColumnSQLTokens(sqlStatementContext, table, hintValueContext.getEncryptColumnItemType()));
        return result;
    }
    
    private Collection<SQLToken> generateProjectionSQLTokens(final SelectStatementContext selectStatementContext, final EncryptTable table) {
        Collection<SQLToken> result = new LinkedList<>();
        for (ProjectionSegment each : selectStatementContext.getSqlStatement().getProjections().getProjections()) {
            if (!(each instanceof ColumnProjectionSegment)) {
                continue;
            }
            IdentifierValue identifier = ((ColumnProjectionSegment) each).getColumn().getIdentifier();
            QuoteCharacter quoteCharacter = identifier.getQuoteCharacter();
            EncryptColumn column = table.getEncryptColumn(identifier.getValue());
            if (column.getPlain().isPresent() && column.getPlain().get().isQueryWithPlain()) {
                Projection projection = new ColumnProjection(null, new IdentifierValue(column.getPlain().get().getName(), quoteCharacter), null, selectStatementContext.getDatabaseType());
                result.add(
                        new SubstitutableColumnNameToken(each.getStartIndex(), each.getStopIndex(), Collections.singleton(projection), selectStatementContext.getDatabaseType(), database, metaData));
            } else {
                Projection projection = new ColumnProjection(null, new IdentifierValue(column.getCipher().getName(), quoteCharacter), null, selectStatementContext.getDatabaseType());
                result.add(
                        new SubstitutableColumnNameToken(each.getStartIndex(), each.getStopIndex(), Collections.singleton(projection), selectStatementContext.getDatabaseType(), database, metaData));
            }
        }
        return result;
    }
    
    private Collection<SQLToken> generateColumnSQLTokens(final SelectStatementContext selectStatementContext, final EncryptTable table, final EncryptColumnItemType jobColumnType) {
        Collection<SQLToken> result = new LinkedList<>();
        for (ColumnSegment each : selectStatementContext.getColumnSegments()) {
            String columnName = each.getIdentifier().getValue();
            if (!table.isEncryptColumn(columnName)) {
                continue;
            }
            EncryptColumn encryptColumn = table.getEncryptColumn(columnName);
            Optional<PlainColumnItem> plainColumnItem = encryptColumn.getPlain();
            QuoteCharacter quoteCharacter = each.getIdentifier().getQuoteCharacter();
            if (plainColumnItem.isPresent() && plainColumnItem.get().isQueryWithPlain()) {
                getJobColumnName(jobColumnType, encryptColumn).ifPresent(
                        optional -> result.add(new NoneUniqueKeyScenarioAddColumnToken(each.getStartIndex(), each.getStopIndex() + 8, optional, plainColumnItem.get().getName(), quoteCharacter)));
            } else {
                getJobColumnName(jobColumnType, encryptColumn).ifPresent(
                        optional -> result.add(new NoneUniqueKeyScenarioAddColumnToken(each.getStartIndex(), each.getStopIndex() + 8, optional, encryptColumn.getCipher().getName(), quoteCharacter)));
            }
        }
        return result;
    }
    
    private Optional<String> getJobColumnName(final EncryptColumnItemType jobColumnType, final EncryptColumn encryptColumn) {
        if (EncryptColumnItemType.CIPHER == jobColumnType) {
            return Optional.ofNullable(encryptColumn.getCipher().getName());
        }
        if (EncryptColumnItemType.LIKE_QUERY == jobColumnType) {
            return encryptColumn.getLikeQuery().map(LikeQueryColumnItem::getName);
        }
        if (EncryptColumnItemType.ASSISTED_QUERY == jobColumnType) {
            return encryptColumn.getAssistedQuery().map(AssistedQueryColumnItem::getName);
        }
        if (EncryptColumnItemType.ORDER_QUERY == jobColumnType) {
            return encryptColumn.getOrderQuery().map(OrderQueryColumnItem::getName);
        }
        return Optional.empty();
    }
}
