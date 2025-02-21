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

package org.apache.shardingsphere.encrypt.rewrite.token.generator.insert;

import com.google.common.base.Preconditions;
import com.sphereex.dbplusengine.SphereEx;
import com.sphereex.dbplusengine.SphereEx.Type;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import org.apache.shardingsphere.encrypt.checker.cryptographic.InsertSelectColumnsEncryptorChecker;
import org.apache.shardingsphere.encrypt.rule.EncryptRule;
import org.apache.shardingsphere.encrypt.rule.column.EncryptColumn;
import org.apache.shardingsphere.encrypt.rule.table.EncryptTable;
import org.apache.shardingsphere.infra.annotation.HighFrequencyInvocation;
import org.apache.shardingsphere.infra.binder.context.segment.select.projection.Projection;
import org.apache.shardingsphere.infra.binder.context.statement.SQLStatementContext;
import org.apache.shardingsphere.infra.binder.context.statement.dml.InsertStatementContext;
import org.apache.shardingsphere.infra.database.core.metadata.database.enums.QuoteCharacter;
import org.apache.shardingsphere.infra.database.core.type.DatabaseTypeRegistry;
import org.apache.shardingsphere.infra.exception.core.ShardingSpherePreconditions;
import org.apache.shardingsphere.infra.exception.generic.UnsupportedSQLOperationException;
import org.apache.shardingsphere.infra.rewrite.sql.token.common.generator.OptionalSQLTokenGenerator;
import org.apache.shardingsphere.infra.rewrite.sql.token.common.generator.aware.PreviousSQLTokensAware;
import org.apache.shardingsphere.infra.rewrite.sql.token.common.pojo.SQLToken;
import org.apache.shardingsphere.infra.rewrite.sql.token.common.pojo.generic.UseDefaultInsertColumnsToken;
import org.apache.shardingsphere.sql.parser.statement.core.segment.dml.column.ColumnSegment;
import org.apache.shardingsphere.sql.parser.statement.core.segment.dml.column.InsertColumnsSegment;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Insert default columns token generator for encrypt.
 */
@HighFrequencyInvocation
@RequiredArgsConstructor
@Setter
public final class EncryptInsertDefaultColumnsTokenGenerator implements OptionalSQLTokenGenerator<InsertStatementContext>, PreviousSQLTokensAware {
    
    private final EncryptRule rule;
    
    @SphereEx
    private final Map<String, EncryptRule> databaseEncryptRules;
    
    private List<SQLToken> previousSQLTokens;
    
    @SphereEx(Type.MODIFY)
    @Override
    public boolean isGenerateSQLToken(final SQLStatementContext sqlStatementContext) {
        return sqlStatementContext instanceof InsertStatementContext && !((InsertStatementContext) sqlStatementContext).containsInsertColumns()
                && ((InsertStatementContext) sqlStatementContext).getMultiInsertStatementContexts().isEmpty();
    }
    
    @Override
    public UseDefaultInsertColumnsToken generateSQLToken(final InsertStatementContext insertStatementContext) {
        String tableName = insertStatementContext.getSqlStatement().getTable().map(optional -> optional.getTableName().getIdentifier().getValue()).orElse("");
        Optional<UseDefaultInsertColumnsToken> previousSQLToken = findInsertColumnsToken();
        if (previousSQLToken.isPresent()) {
            processPreviousSQLToken(previousSQLToken.get(), insertStatementContext, tableName);
            return previousSQLToken.get();
        }
        return generateNewSQLToken(insertStatementContext, tableName);
    }
    
    private Optional<UseDefaultInsertColumnsToken> findInsertColumnsToken() {
        for (SQLToken each : previousSQLTokens) {
            if (each instanceof UseDefaultInsertColumnsToken) {
                return Optional.of((UseDefaultInsertColumnsToken) each);
            }
        }
        return Optional.empty();
    }
    
    private void processPreviousSQLToken(final UseDefaultInsertColumnsToken previousSQLToken, final InsertStatementContext insertStatementContext, final String tableName) {
        List<String> columnNames = getColumnNames(insertStatementContext, rule.getEncryptTable(tableName), previousSQLToken.getColumns());
        previousSQLToken.getColumns().clear();
        previousSQLToken.getColumns().addAll(columnNames);
    }
    
    private UseDefaultInsertColumnsToken generateNewSQLToken(final InsertStatementContext insertStatementContext, final String tableName) {
        Optional<InsertColumnsSegment> insertColumnsSegment = insertStatementContext.getSqlStatement().getInsertColumns();
        Preconditions.checkState(insertColumnsSegment.isPresent());
        if (null != insertStatementContext.getInsertSelectContext()) {
            Collection<ColumnSegment> derivedInsertColumns = insertStatementContext.getSqlStatement().getDerivedInsertColumns();
            Collection<Projection> projections = insertStatementContext.getInsertSelectContext().getSelectStatementContext().getProjectionsContext().getExpandProjections();
            ShardingSpherePreconditions.checkState(derivedInsertColumns.size() == projections.size(), () -> new UnsupportedSQLOperationException("Column count doesn't match value count."));
            // SPEX CHANGED: BEGIN
            InsertSelectColumnsEncryptorChecker.checkIsSame(derivedInsertColumns, projections, rule, databaseEncryptRules);
            // SPEX CHANGED: END
        }
        // SPEX CHANGED: BEGIN
        QuoteCharacter quoteCharacter = new DatabaseTypeRegistry(insertStatementContext.getDatabaseType()).getDialectDatabaseMetaData().getQuoteCharacter();
        UseDefaultInsertColumnsToken result = new UseDefaultInsertColumnsToken(insertColumnsSegment.get().getStopIndex(), new ArrayList<>(insertStatementContext.getColumnNames()), quoteCharacter);
        generateColumnNames(insertStatementContext, rule.getEncryptTable(tableName), result);
        return result;
        // SPEX CHANGED: END
    }
    
    @SphereEx
    private void generateColumnNames(final InsertStatementContext sqlStatementContext, final EncryptTable encryptTable, final UseDefaultInsertColumnsToken token) {
        List<String> columnNames = token.getColumns();
        Iterator<String> descendingColumnNames = sqlStatementContext.getDescendingColumnNames();
        while (descendingColumnNames.hasNext()) {
            String columnName = descendingColumnNames.next();
            if (!encryptTable.isEncryptColumn(columnName)) {
                continue;
            }
            EncryptColumn encryptColumn = encryptTable.getEncryptColumn(columnName);
            int columnIndex = columnNames.indexOf(columnName);
            setCipherColumn(columnNames, encryptColumn, columnIndex);
            if (encryptColumn.getAssistedQuery().isPresent()) {
                token.addAddedValue(columnIndex, encryptColumn.getAssistedQuery().get().getName());
            }
            if (encryptColumn.getLikeQuery().isPresent()) {
                token.addAddedValue(columnIndex, encryptColumn.getLikeQuery().get().getName());
            }
            if (encryptColumn.getOrderQuery().isPresent()) {
                token.addAddedValue(columnIndex, encryptColumn.getOrderQuery().get().getName());
            }
            if (encryptColumn.getPlain().isPresent()) {
                token.addAddedValue(columnIndex, encryptColumn.getPlain().get().getName());
            }
        }
    }
    
    private List<String> getColumnNames(final InsertStatementContext sqlStatementContext, final EncryptTable encryptTable, final List<String> currentColumnNames) {
        List<String> result = new LinkedList<>(currentColumnNames);
        Iterator<String> descendingColumnNames = sqlStatementContext.getDescendingColumnNames();
        while (descendingColumnNames.hasNext()) {
            String columnName = descendingColumnNames.next();
            if (!encryptTable.isEncryptColumn(columnName)) {
                continue;
            }
            EncryptColumn encryptColumn = encryptTable.getEncryptColumn(columnName);
            int columnIndex = result.indexOf(columnName);
            setCipherColumn(result, encryptColumn, columnIndex);
            if (encryptColumn.getAssistedQuery().isPresent()) {
                addAssistedQueryColumn(result, encryptColumn, columnIndex);
                columnIndex++;
            }
            if (encryptColumn.getLikeQuery().isPresent()) {
                addLikeQueryColumn(result, encryptColumn, columnIndex);
                // SPEX ADDED: BEGIN
                columnIndex++;
                // SPEX ADDED: END
            }
            // SPEX ADDED: BEGIN
            if (encryptColumn.getOrderQuery().isPresent()) {
                addOrderQueryColumn(result, encryptColumn, columnIndex);
                columnIndex++;
            }
            if (encryptColumn.getPlain().isPresent()) {
                addPlainColumn(result, encryptColumn, columnIndex);
            }
            // SPEX ADDED: END
        }
        return result;
    }
    
    private void setCipherColumn(final List<String> columnNames, final EncryptColumn encryptColumn, final int columnIndex) {
        columnNames.set(columnIndex, encryptColumn.getCipher().getName());
    }
    
    private void addAssistedQueryColumn(final List<String> columnNames, final EncryptColumn encryptColumn, final int columnIndex) {
        encryptColumn.getAssistedQuery().ifPresent(optional -> columnNames.add(columnIndex + 1, optional.getName()));
    }
    
    private void addLikeQueryColumn(final List<String> columnNames, final EncryptColumn encryptColumn, final int columnIndex) {
        encryptColumn.getLikeQuery().ifPresent(optional -> columnNames.add(columnIndex + 1, optional.getName()));
    }
    
    @SphereEx
    private void addOrderQueryColumn(final List<String> columnNames, final EncryptColumn encryptColumn, final int columnIndex) {
        encryptColumn.getOrderQuery().ifPresent(optional -> columnNames.add(columnIndex + 1, optional.getName()));
    }
    
    @SphereEx
    private void addPlainColumn(final List<String> columnNames, final EncryptColumn encryptColumn, final int columnIndex) {
        encryptColumn.getPlain().ifPresent(optional -> columnNames.add(columnIndex + 1, optional.getName()));
    }
}
