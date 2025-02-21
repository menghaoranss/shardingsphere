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
import com.sphereex.dbplusengine.infra.hint.NoneUniqueKeyScenario;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import org.apache.shardingsphere.encrypt.rule.EncryptRule;
import org.apache.shardingsphere.encrypt.rule.column.EncryptColumn;
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

/**
 * Select token generator for decrypt job.
 */
@HighFrequencyInvocation
@RequiredArgsConstructor
@Setter
public final class DecryptSelectTokenGenerator implements CollectionSQLTokenGenerator<SelectStatementContext>, HintValueContextAware {
    
    private final EncryptRule rule;
    
    private final ShardingSphereMetaData metaData;
    
    private ShardingSphereDatabase database;
    
    private HintValueContext hintValueContext;
    
    @Override
    public boolean isGenerateSQLToken(final SQLStatementContext sqlStatementContext) {
        return sqlStatementContext instanceof SelectStatementContext && !((SelectStatementContext) sqlStatementContext).getTablesContext().getSimpleTables().isEmpty()
                && NoneUniqueKeyScenario.DECRYPT == hintValueContext.getNoneUniqueKeyScenario();
    }
    
    @Override
    public Collection<SQLToken> generateSQLTokens(final SelectStatementContext sqlStatementContext) {
        EncryptTable table = rule.getEncryptTable(sqlStatementContext.getTablesContext().getTableNames().stream().findFirst().orElse(null));
        Collection<SQLToken> result = generateProjectionSQLTokens(sqlStatementContext, table);
        result.addAll(generateColumnSQLTokens(sqlStatementContext, table));
        return result;
    }
    
    private Collection<SQLToken> generateProjectionSQLTokens(final SelectStatementContext selectStatementContext, final EncryptTable table) {
        Collection<SQLToken> result = new LinkedList<>();
        for (ProjectionSegment each : selectStatementContext.getSqlStatement().getProjections().getProjections()) {
            IdentifierValue identifier = ((ColumnProjectionSegment) each).getColumn().getIdentifier();
            String columnName = identifier.getValue();
            QuoteCharacter quoteCharacter = identifier.getQuoteCharacter();
            if (table.isEncryptColumn(columnName)) {
                Projection projection =
                        new ColumnProjection(null, new IdentifierValue(table.getEncryptColumn(columnName).getCipher().getName(), quoteCharacter), null, selectStatementContext.getDatabaseType());
                result.add(
                        new SubstitutableColumnNameToken(each.getStartIndex(), each.getStopIndex(), Collections.singleton(projection), selectStatementContext.getDatabaseType(), database, metaData));
            }
        }
        return result;
    }
    
    private Collection<SQLToken> generateColumnSQLTokens(final SelectStatementContext selectStatementContext, final EncryptTable table) {
        Collection<SQLToken> result = new LinkedList<>();
        for (ColumnSegment each : selectStatementContext.getColumnSegments()) {
            String columnName = each.getIdentifier().getValue();
            if (!table.isEncryptColumn(columnName)) {
                continue;
            }
            EncryptColumn column = table.getEncryptColumn(columnName);
            column.getPlain().ifPresent(optional -> result.add(new NoneUniqueKeyScenarioAddColumnToken(each.getStartIndex(),
                    each.getStopIndex() + 8, optional.getName(), column.getCipher().getName(), each.getIdentifier().getQuoteCharacter())));
        }
        return result;
    }
}
