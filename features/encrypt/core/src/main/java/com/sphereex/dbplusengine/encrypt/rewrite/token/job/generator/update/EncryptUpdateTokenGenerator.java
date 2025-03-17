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

package com.sphereex.dbplusengine.encrypt.rewrite.token.job.generator.update;

import com.sphereex.dbplusengine.encrypt.rewrite.aware.HintValueContextAware;
import com.sphereex.dbplusengine.encrypt.rewrite.token.job.pojo.update.NoneUniqueKeyScenarioUpdateColumnToken;
import com.sphereex.dbplusengine.encrypt.rule.column.item.PlainColumnItem;
import com.sphereex.dbplusengine.infra.hint.EncryptColumnItemType;
import com.sphereex.dbplusengine.infra.hint.NoneUniqueKeyScenario;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import org.apache.shardingsphere.encrypt.rewrite.token.pojo.EncryptAssignmentToken;
import org.apache.shardingsphere.encrypt.rewrite.token.pojo.EncryptParameterAssignmentToken;
import org.apache.shardingsphere.encrypt.rule.EncryptRule;
import org.apache.shardingsphere.encrypt.rule.column.EncryptColumn;
import org.apache.shardingsphere.encrypt.rule.table.EncryptTable;
import org.apache.shardingsphere.infra.annotation.HighFrequencyInvocation;
import org.apache.shardingsphere.infra.binder.context.segment.select.projection.Projection;
import org.apache.shardingsphere.infra.binder.context.segment.select.projection.impl.ColumnProjection;
import org.apache.shardingsphere.infra.binder.context.statement.SQLStatementContext;
import org.apache.shardingsphere.infra.binder.context.statement.dml.UpdateStatementContext;
import org.apache.shardingsphere.infra.database.core.metadata.database.enums.QuoteCharacter;
import org.apache.shardingsphere.infra.hint.HintValueContext;
import org.apache.shardingsphere.infra.metadata.ShardingSphereMetaData;
import org.apache.shardingsphere.infra.metadata.database.ShardingSphereDatabase;
import org.apache.shardingsphere.infra.rewrite.sql.token.common.generator.CollectionSQLTokenGenerator;
import org.apache.shardingsphere.infra.rewrite.sql.token.common.pojo.SQLToken;
import org.apache.shardingsphere.infra.rewrite.sql.token.common.pojo.generic.SubstitutableColumnNameToken;
import org.apache.shardingsphere.sql.parser.statement.core.segment.dml.assignment.ColumnAssignmentSegment;
import org.apache.shardingsphere.sql.parser.statement.core.segment.dml.assignment.SetAssignmentSegment;
import org.apache.shardingsphere.sql.parser.statement.core.segment.dml.column.ColumnSegment;
import org.apache.shardingsphere.sql.parser.statement.core.segment.dml.predicate.WhereSegment;
import org.apache.shardingsphere.sql.parser.statement.core.value.identifier.IdentifierValue;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.Optional;

/**
 * Update token generator for encrypt.
 */
@HighFrequencyInvocation
@RequiredArgsConstructor
@Setter
public final class EncryptUpdateTokenGenerator implements CollectionSQLTokenGenerator<UpdateStatementContext>, HintValueContextAware {
    
    private final EncryptRule rule;
    
    private final ShardingSphereMetaData metaData;
    
    private final ShardingSphereDatabase database;
    
    private HintValueContext hintValueContext;
    
    @Override
    public boolean isGenerateSQLToken(final SQLStatementContext sqlStatementContext) {
        return sqlStatementContext instanceof UpdateStatementContext && !((UpdateStatementContext) sqlStatementContext).getTablesContext().getSimpleTables().isEmpty()
                && NoneUniqueKeyScenario.ENCRYPT == hintValueContext.getNoneUniqueKeyScenario();
    }
    
    @Override
    public Collection<SQLToken> generateSQLTokens(final UpdateStatementContext sqlStatementContext) {
        EncryptTable table = rule.getEncryptTable(sqlStatementContext.getTablesContext().getTableNames().stream().findFirst().orElse(null));
        Collection<SQLToken> result = generateColumnSQLTokens(sqlStatementContext, table);
        result.addAll(generateAssignmentSQLTokens(sqlStatementContext, table));
        return result;
    }
    
    private Collection<SQLToken> generateColumnSQLTokens(final UpdateStatementContext sqlStatementContext, final EncryptTable table) {
        Collection<SQLToken> result = new LinkedList<>();
        for (ColumnSegment each : sqlStatementContext.getColumnSegments()) {
            IdentifierValue identifier = each.getIdentifier();
            String columnName = identifier.getValue();
            if (!table.isEncryptColumn(columnName)) {
                continue;
            }
            Optional<PlainColumnItem> plainColumn = table.getEncryptColumn(columnName).getPlain();
            QuoteCharacter quoteCharacter = identifier.getQuoteCharacter();
            if (plainColumn.isPresent() && plainColumn.get().isQueryWithPlain()) {
                Projection projection = new ColumnProjection(null, new IdentifierValue(plainColumn.get().getName(), quoteCharacter), null, sqlStatementContext.getDatabaseType());
                result.add(new SubstitutableColumnNameToken(each.getStartIndex(), each.getStopIndex(), Collections.singleton(projection), sqlStatementContext.getDatabaseType(), database, metaData));
            } else {
                Projection projection =
                        new ColumnProjection(null, new IdentifierValue(table.getEncryptColumn(columnName).getCipher().getName(), quoteCharacter), null, sqlStatementContext.getDatabaseType());
                result.add(new SubstitutableColumnNameToken(each.getStartIndex(), each.getStopIndex(), Collections.singleton(projection), sqlStatementContext.getDatabaseType(), database, metaData));
            }
        }
        return result;
    }
    
    private Collection<SQLToken> generateAssignmentSQLTokens(final UpdateStatementContext sqlStatementContext, final EncryptTable table) {
        Optional<SetAssignmentSegment> assignmentSegment = sqlStatementContext.getSqlStatement().getAssignmentSegment();
        if (!assignmentSegment.isPresent()) {
            return Collections.emptyList();
        }
        Collection<SQLToken> result = new LinkedList<>();
        for (ColumnAssignmentSegment each : assignmentSegment.get().getAssignments()) {
            IdentifierValue identifier = each.getColumns().get(0).getIdentifier();
            result.add(generateLiteralSQLToken(table.getEncryptColumn(identifier.getValue()), each));
            result.add(getEncryptJobUpdateTokenGenerator(sqlStatementContext, table.getEncryptColumn(identifier.getValue()), identifier.getQuoteCharacter()));
        }
        return result;
    }
    
    private EncryptAssignmentToken generateLiteralSQLToken(final EncryptColumn encryptColumn, final ColumnAssignmentSegment assignmentSegment) {
        EncryptParameterAssignmentToken result = new EncryptParameterAssignmentToken(assignmentSegment.getColumns().get(0).getStartIndex(), assignmentSegment.getStopIndex(),
                assignmentSegment.getColumns().get(0).getIdentifier().getQuoteCharacter(), null);
        result.addColumnName(encryptColumn.getCipher().getName());
        encryptColumn.getAssistedQuery().ifPresent(optional -> result.addColumnName(optional.getName()));
        encryptColumn.getLikeQuery().ifPresent(optional -> result.addColumnName(optional.getName()));
        encryptColumn.getOrderQuery().ifPresent(optional -> result.addColumnName(optional.getName()));
        encryptColumn.getPlain().ifPresent(optional -> result.addColumnName(optional.getName()));
        return result;
    }
    
    private NoneUniqueKeyScenarioUpdateColumnToken getEncryptJobUpdateTokenGenerator(final UpdateStatementContext statementContext, final EncryptColumn encryptColumn,
                                                                                     final QuoteCharacter quoteCharacter) {
        WhereSegment whereSegment = statementContext.getWhereSegments().iterator().next();
        if (EncryptColumnItemType.LIKE_QUERY == hintValueContext.getEncryptColumnItemType() && encryptColumn.getLikeQuery().isPresent()) {
            return new NoneUniqueKeyScenarioUpdateColumnToken(whereSegment.getStopIndex() + 1, whereSegment.getStopIndex(), encryptColumn.getLikeQuery().get().getName(), quoteCharacter);
        }
        if (EncryptColumnItemType.ORDER_QUERY == hintValueContext.getEncryptColumnItemType() && encryptColumn.getOrderQuery().isPresent()) {
            return new NoneUniqueKeyScenarioUpdateColumnToken(whereSegment.getStopIndex() + 1, whereSegment.getStopIndex(), encryptColumn.getOrderQuery().get().getName(), quoteCharacter);
        }
        return new NoneUniqueKeyScenarioUpdateColumnToken(whereSegment.getStopIndex() + 1, whereSegment.getStopIndex(), encryptColumn.getCipher().getName(), quoteCharacter);
    }
}
