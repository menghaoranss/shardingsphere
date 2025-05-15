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

package com.sphereex.dbplusengine.encrypt.rewrite.token.cryptographic.generator.merge;

import com.google.common.base.Preconditions;
import com.sphereex.dbplusengine.SphereEx;
import com.sphereex.dbplusengine.SphereEx.Type;
import com.sphereex.dbplusengine.encrypt.rewrite.token.cryptographic.generator.orderby.EncryptOrderByItemTokenGenerator;
import com.sphereex.dbplusengine.encrypt.rewrite.token.cryptographic.generator.select.EncryptSelectForUpdateTokenGenerator;
import com.sphereex.dbplusengine.encrypt.rewrite.token.cryptographic.generator.select.EncryptUsingNaturalJoinTokenGenerator;
import com.sphereex.dbplusengine.encrypt.rewrite.token.cryptographic.pojo.EncryptColumnAssignmentToken;
import com.sphereex.dbplusengine.encrypt.rule.column.item.OrderQueryColumnItem;
import com.sphereex.dbplusengine.infra.binder.context.statement.dml.MergeStatementContext;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import org.apache.shardingsphere.encrypt.checker.cryptographic.JoinConditionsEncryptorChecker;
import org.apache.shardingsphere.encrypt.rewrite.condition.EncryptCondition;
import org.apache.shardingsphere.encrypt.rewrite.condition.EncryptConditionEngine;
import org.apache.shardingsphere.encrypt.rewrite.condition.EncryptConditionValues;
import org.apache.shardingsphere.encrypt.rewrite.condition.impl.EncryptBinaryCondition;
import org.apache.shardingsphere.encrypt.rewrite.condition.impl.EncryptInCondition;
import org.apache.shardingsphere.encrypt.rewrite.token.generator.insert.EncryptInsertCipherNameTokenGenerator;
import org.apache.shardingsphere.encrypt.rewrite.token.generator.insert.EncryptInsertDefaultColumnsTokenGenerator;
import org.apache.shardingsphere.encrypt.rewrite.token.generator.insert.EncryptInsertDerivedColumnsTokenGenerator;
import org.apache.shardingsphere.encrypt.rewrite.token.generator.predicate.EncryptPredicateColumnTokenGenerator;
import org.apache.shardingsphere.encrypt.rewrite.token.generator.predicate.EncryptPredicateValueTokenGenerator;
import org.apache.shardingsphere.encrypt.rewrite.token.generator.projection.EncryptSelectProjectionTokenGenerator;
import org.apache.shardingsphere.encrypt.rewrite.token.generator.select.EncryptGroupByItemTokenGenerator;
import org.apache.shardingsphere.encrypt.rewrite.token.pojo.EncryptAssignmentToken;
import org.apache.shardingsphere.encrypt.rewrite.token.pojo.EncryptInsertValuesToken;
import org.apache.shardingsphere.encrypt.rewrite.token.pojo.EncryptLiteralAssignmentToken;
import org.apache.shardingsphere.encrypt.rewrite.token.pojo.EncryptParameterAssignmentToken;
import org.apache.shardingsphere.encrypt.rewrite.token.pojo.EncryptPredicateEqualRightValueToken;
import org.apache.shardingsphere.encrypt.rewrite.token.pojo.EncryptPredicateFunctionRightValueToken;
import org.apache.shardingsphere.encrypt.rewrite.token.pojo.EncryptPredicateInRightValueToken;
import org.apache.shardingsphere.encrypt.rule.EncryptRule;
import org.apache.shardingsphere.encrypt.rule.column.EncryptColumn;
import org.apache.shardingsphere.encrypt.rule.column.item.AssistedQueryColumnItem;
import org.apache.shardingsphere.encrypt.rule.column.item.LikeQueryColumnItem;
import org.apache.shardingsphere.encrypt.rule.table.EncryptTable;
import org.apache.shardingsphere.infra.binder.context.segment.insert.values.InsertValueContext;
import org.apache.shardingsphere.infra.binder.context.segment.insert.values.expression.DerivedLiteralExpressionSegment;
import org.apache.shardingsphere.infra.binder.context.segment.insert.values.expression.DerivedParameterMarkerExpressionSegment;
import org.apache.shardingsphere.infra.binder.context.segment.select.projection.Projection;
import org.apache.shardingsphere.infra.binder.context.segment.select.projection.impl.ColumnProjection;
import org.apache.shardingsphere.infra.binder.context.statement.SQLStatementContext;
import org.apache.shardingsphere.infra.binder.context.statement.dml.InsertStatementContext;
import org.apache.shardingsphere.infra.binder.context.statement.dml.SelectStatementContext;
import org.apache.shardingsphere.infra.database.core.metadata.database.enums.QuoteCharacter;
import org.apache.shardingsphere.infra.database.core.type.DatabaseType;
import org.apache.shardingsphere.infra.database.core.type.DatabaseTypeRegistry;
import org.apache.shardingsphere.infra.exception.core.ShardingSpherePreconditions;
import org.apache.shardingsphere.infra.exception.generic.UnsupportedSQLOperationException;
import org.apache.shardingsphere.infra.metadata.ShardingSphereMetaData;
import org.apache.shardingsphere.infra.metadata.database.ShardingSphereDatabase;
import org.apache.shardingsphere.infra.rewrite.sql.token.common.generator.CollectionSQLTokenGenerator;
import org.apache.shardingsphere.infra.rewrite.sql.token.common.generator.aware.ParametersAware;
import org.apache.shardingsphere.infra.rewrite.sql.token.common.generator.aware.PreviousSQLTokensAware;
import org.apache.shardingsphere.infra.rewrite.sql.token.common.pojo.SQLToken;
import org.apache.shardingsphere.infra.rewrite.sql.token.common.pojo.generic.InsertValue;
import org.apache.shardingsphere.infra.rewrite.sql.token.common.pojo.generic.InsertValuesToken;
import org.apache.shardingsphere.infra.rewrite.sql.token.common.pojo.generic.SubstitutableColumnNameToken;
import org.apache.shardingsphere.sql.parser.statement.core.extractor.ColumnExtractor;
import org.apache.shardingsphere.sql.parser.statement.core.extractor.ExpressionExtractor;
import org.apache.shardingsphere.sql.parser.statement.core.segment.dml.assignment.ColumnAssignmentSegment;
import org.apache.shardingsphere.sql.parser.statement.core.segment.dml.assignment.InsertValuesSegment;
import org.apache.shardingsphere.sql.parser.statement.core.segment.dml.assignment.SetAssignmentSegment;
import org.apache.shardingsphere.sql.parser.statement.core.segment.dml.column.ColumnSegment;
import org.apache.shardingsphere.sql.parser.statement.core.segment.dml.expr.BinaryOperationExpression;
import org.apache.shardingsphere.sql.parser.statement.core.segment.dml.expr.ExpressionSegment;
import org.apache.shardingsphere.sql.parser.statement.core.segment.dml.expr.FunctionSegment;
import org.apache.shardingsphere.sql.parser.statement.core.segment.dml.expr.simple.LiteralExpressionSegment;
import org.apache.shardingsphere.sql.parser.statement.core.segment.dml.expr.simple.ParameterMarkerExpressionSegment;
import org.apache.shardingsphere.sql.parser.statement.core.segment.dml.predicate.WhereSegment;
import org.apache.shardingsphere.sql.parser.statement.core.segment.generic.OwnerSegment;
import org.apache.shardingsphere.sql.parser.statement.core.segment.generic.bound.ColumnSegmentBoundInfo;
import org.apache.shardingsphere.sql.parser.statement.core.segment.generic.table.SimpleTableSegment;
import org.apache.shardingsphere.sql.parser.statement.core.segment.generic.table.TableSegment;
import org.apache.shardingsphere.sql.parser.statement.core.statement.dml.InsertStatement;
import org.apache.shardingsphere.sql.parser.statement.core.statement.dml.MergeStatement;
import org.apache.shardingsphere.sql.parser.statement.core.statement.dml.UpdateStatement;
import org.apache.shardingsphere.sql.parser.statement.core.value.identifier.IdentifierValue;
import org.apache.shardingsphere.sql.parser.statement.oracle.dml.OracleUpdateStatement;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Encrypt merge token generator.
 */
@RequiredArgsConstructor
@Setter
public final class EncryptMergeTokenGenerator implements CollectionSQLTokenGenerator<MergeStatementContext>, PreviousSQLTokensAware, ParametersAware {
    
    private final EncryptRule rule;
    
    private final Map<String, EncryptRule> databaseEncryptRules;
    
    private final ShardingSphereDatabase database;
    
    private final ShardingSphereMetaData metaData;
    
    private List<Object> parameters;
    
    private List<SQLToken> previousSQLTokens;
    
    @Override
    public boolean isGenerateSQLToken(final SQLStatementContext sqlStatementContext) {
        return sqlStatementContext instanceof MergeStatementContext;
    }
    
    @Override
    public Collection<SQLToken> generateSQLTokens(final MergeStatementContext mergeStatementContext) {
        Collection<SQLToken> result = generateForCondition(mergeStatementContext);
        mergeStatementContext.getSourceSubQueryStatement().ifPresent(selectStatementContext -> result.addAll(generateSQLTokensForSelect(selectStatementContext)));
        if (mergeStatementContext.getSqlStatement().getUpdate().isPresent()) {
            result.addAll(generateSQLTokensForUpdate(mergeStatementContext));
        }
        if (mergeStatementContext.getSqlStatement().getInsert().isPresent()) {
            result.addAll(generateSQLTokensForInsert(mergeStatementContext));
        }
        return result;
    }
    
    private Collection<SQLToken> generateForCondition(final MergeStatementContext mergeStatementContext) {
        ExpressionSegment condition = mergeStatementContext.getSqlStatement().getExpression().getExpr();
        Collection<BinaryOperationExpression> joinConditions = new LinkedList<>();
        ExpressionExtractor.extractJoinConditions(joinConditions, Collections.singleton(new WhereSegment(0, 0, condition)));
        JoinConditionsEncryptorChecker.checkIsSame(joinConditions, rule, databaseEncryptRules, "using condition");
        Collection<ColumnSegment> columnSegments = new LinkedList<>();
        ColumnExtractor.extractColumnSegments(columnSegments, Collections.singleton(new WhereSegment(0, 0, condition)));
        Collection<SQLToken> result = new LinkedList<>(generateSQLTokensForPredicateColumns(columnSegments, Collections.singleton(condition), mergeStatementContext.getDatabaseType()));
        result.addAll(generateSQLTokensForPredicateRightValue(condition, columnSegments, mergeStatementContext));
        return result;
    }
    
    private Collection<SQLToken> generateSQLTokensForPredicateRightValue(final ExpressionSegment condition, final Collection<ColumnSegment> columnSegments,
                                                                         final MergeStatementContext mergeStatementContext) {
        Collection<EncryptCondition> encryptConditions = new EncryptConditionEngine(rule, databaseEncryptRules)
                .createEncryptConditions(Collections.singleton(new WhereSegment(condition.getStartIndex(), condition.getStopIndex(), condition)));
        String schemaName = mergeStatementContext.getTablesContext().getSchemaName()
                .orElseGet(() -> new DatabaseTypeRegistry(mergeStatementContext.getDatabaseType()).getDefaultSchemaName(database.getName()));
        return generatePredicateRightValueForConditions(encryptConditions, schemaName);
    }
    
    private Collection<SQLToken> generatePredicateRightValueForConditions(final Collection<EncryptCondition> encryptConditions, final String schemaName) {
        Collection<SQLToken> result = new LinkedList<>();
        for (EncryptCondition each : encryptConditions) {
            ColumnSegmentBoundInfo columnBoundInfo = each.getColumnSegment().getColumnBoundInfo();
            String tableName = columnBoundInfo.getOriginalTable().getValue();
            String databaseName = columnBoundInfo.getOriginalDatabase().getValue().isEmpty() ? database.getName() : columnBoundInfo.getOriginalDatabase().getValue();
            Optional<EncryptTable> databaseEncryptTable = Optional.ofNullable(databaseEncryptRules.get(databaseName)).flatMap(rule -> rule.findEncryptTable(tableName));
            Optional<EncryptTable> encryptTable = Optional.ofNullable(databaseEncryptTable.orElseGet(() -> rule.findEncryptTable(each.getTableName()).orElse(null)));
            encryptTable.ifPresent(optional -> result.add(generatePredicateRightValueEachCondition(schemaName, optional, each)));
        }
        return result;
    }
    
    private SQLToken generatePredicateRightValueEachCondition(final String schemaName, final EncryptTable encryptTable, final EncryptCondition encryptCondition) {
        List<Object> originalValues = new EncryptConditionValues(encryptCondition).get(parameters);
        int startIndex = encryptCondition.getStartIndex();
        boolean queryWithPlain = rule.isQueryWithPlain(encryptCondition.getTableName(), encryptCondition.getColumnSegment().getIdentifier().getValue());
        if (queryWithPlain) {
            return generateSQLTokenForPlainColumn(schemaName, encryptTable, encryptCondition, originalValues, startIndex);
        }
        return generateSQLToken(schemaName, encryptTable, encryptCondition, originalValues, startIndex);
    }
    
    private SQLToken generateSQLToken(final String schemaName, final EncryptTable encryptTable, final EncryptCondition encryptCondition, final List<Object> originalValues, final int startIndex) {
        int stopIndex = encryptCondition.getStopIndex();
        Map<Integer, Object> indexValues = getPositionValues(encryptCondition.getPositionValueMap().keySet(), getEncryptedValues(schemaName, encryptTable, encryptCondition, originalValues));
        Collection<Integer> parameterMarkerIndexes = encryptCondition.getPositionIndexMap().keySet();
        if (encryptCondition instanceof EncryptBinaryCondition && ((EncryptBinaryCondition) encryptCondition).getExpressionSegment() instanceof FunctionSegment) {
            FunctionSegment functionSegment = (FunctionSegment) ((EncryptBinaryCondition) encryptCondition).getExpressionSegment();
            return new EncryptPredicateFunctionRightValueToken(startIndex, stopIndex, functionSegment.getFunctionName(), functionSegment.getParameters(), indexValues, parameterMarkerIndexes);
        }
        return encryptCondition instanceof EncryptInCondition
                ? new EncryptPredicateInRightValueToken(startIndex, stopIndex, indexValues, parameterMarkerIndexes)
                : new EncryptPredicateEqualRightValueToken(startIndex, stopIndex, indexValues, parameterMarkerIndexes);
    }
    
    private List<Object> getEncryptedValues(final String schemaName, final EncryptTable encryptTable, final EncryptCondition encryptCondition, final List<Object> originalValues) {
        EncryptColumn encryptColumn = encryptTable.getEncryptColumn(encryptCondition.getColumnSegment().getIdentifier().getValue());
        if (encryptCondition instanceof EncryptBinaryCondition && "LIKE".equalsIgnoreCase(((EncryptBinaryCondition) encryptCondition).getOperator())) {
            LikeQueryColumnItem likeQueryColumnItem = encryptColumn.getLikeQuery().orElseThrow(() -> new UnsupportedSQLOperationException("LIKE"));
            return likeQueryColumnItem.encrypt(database.getName(), schemaName, encryptCondition.getColumnSegment().getColumnBoundInfo().getOriginalTable().getValue(),
                    encryptCondition.getColumnSegment().getIdentifier().getValue(), originalValues);
        }
        return encryptColumn.getAssistedQuery()
                .map(optional -> optional.encrypt(database.getName(), schemaName, encryptCondition.getColumnSegment().getColumnBoundInfo().getOriginalTable().getValue(),
                        encryptCondition.getColumnSegment().getIdentifier().getValue(), originalValues))
                .orElseGet(() -> encryptColumn.getCipher().encrypt(database.getName(), schemaName, encryptCondition.getColumnSegment().getColumnBoundInfo().getOriginalTable().getValue(),
                        encryptCondition.getColumnSegment().getIdentifier().getValue(), originalValues));
    }
    
    private SQLToken generateSQLTokenForPlainColumn(final String schemaName, final EncryptTable encryptTable, final EncryptCondition encryptCondition,
                                                    final List<Object> originalValues, final int startIndex) {
        int stopIndex = encryptCondition.getStopIndex();
        Map<Integer, Object> indexValues = new HashMap<>();
        if (encryptTable.getEncryptColumn(encryptCondition.getColumnSegment().getIdentifier().getValue()).getPlain().isPresent()) {
            indexValues.putAll(getPositionValues(encryptCondition.getPositionValueMap().keySet(), originalValues));
        } else {
            indexValues.putAll(getPositionValues(encryptCondition.getPositionValueMap().keySet(), getEncryptedValues(schemaName, encryptTable, encryptCondition, originalValues)));
        }
        Collection<Integer> parameterMarkerIndexes = encryptCondition.getPositionIndexMap().keySet();
        if (encryptCondition instanceof EncryptBinaryCondition && ((EncryptBinaryCondition) encryptCondition).getExpressionSegment() instanceof FunctionSegment) {
            FunctionSegment functionSegment = (FunctionSegment) ((EncryptBinaryCondition) encryptCondition).getExpressionSegment();
            return new EncryptPredicateFunctionRightValueToken(startIndex, stopIndex, functionSegment.getFunctionName(), functionSegment.getParameters(), indexValues, parameterMarkerIndexes);
        }
        return encryptCondition instanceof EncryptInCondition
                ? new EncryptPredicateInRightValueToken(startIndex, stopIndex, indexValues, parameterMarkerIndexes)
                : new EncryptPredicateEqualRightValueToken(startIndex, stopIndex, indexValues, parameterMarkerIndexes);
    }
    
    private Map<Integer, Object> getPositionValues(final Collection<Integer> valuePositions, final List<Object> encryptValues) {
        Map<Integer, Object> result = new LinkedHashMap<>();
        for (int each : valuePositions) {
            result.put(each, encryptValues.get(each));
        }
        return result;
    }
    
    private Collection<SQLToken> generateSQLTokensForPredicateColumns(final Collection<ColumnSegment> columnSegments,
                                                                      final Collection<ExpressionSegment> conditions, final DatabaseType databaseType) {
        Collection<SQLToken> result = new LinkedList<>();
        for (ColumnSegment each : columnSegments) {
            if (null != each.getColumnBoundInfo().getOriginalTable() && rule.findEncryptTable(each.getColumnBoundInfo().getOriginalTable().getValue()).isPresent()
                    && null != each.getColumnBoundInfo().getOriginalColumn()
                    && rule.getEncryptTable(each.getColumnBoundInfo().getOriginalTable().getValue()).isEncryptColumn(each.getColumnBoundInfo().getOriginalColumn().getValue())) {
                result.add(generateSQLTokenForColumn(each, conditions, databaseType));
            }
        }
        return result;
    }
    
    private SQLToken generateSQLTokenForColumn(final ColumnSegment columnSegment, final Collection<ExpressionSegment> expressionSegments, final DatabaseType databaseType) {
        EncryptTable encryptTable = rule.getEncryptTable(columnSegment.getColumnBoundInfo().getOriginalTable().getValue());
        EncryptColumn encryptColumn = encryptTable.getEncryptColumn(columnSegment.getColumnBoundInfo().getOriginalColumn().getValue());
        int startIndex = columnSegment.getOwner().isPresent() ? columnSegment.getOwner().get().getStopIndex() + 2 : columnSegment.getStartIndex();
        int stopIndex = columnSegment.getStopIndex();
        if (encryptColumn.getPlain().isPresent() && rule.isQueryWithPlain(encryptTable.getTable(), encryptColumn.getName())) {
            return new SubstitutableColumnNameToken(startIndex, stopIndex,
                    createColumnProjections(encryptColumn.getPlain().get().getName(), columnSegment.getIdentifier().getQuoteCharacter(), databaseType), databaseType, database, metaData);
        }
        if (includesLike(expressionSegments, columnSegment)) {
            LikeQueryColumnItem likeQueryColumnItem = encryptColumn.getLikeQuery().orElseThrow(() -> new UnsupportedSQLOperationException("LIKE"));
            return new SubstitutableColumnNameToken(startIndex, stopIndex, createColumnProjections(likeQueryColumnItem.getName(), columnSegment.getIdentifier().getQuoteCharacter(), databaseType),
                    databaseType, database, metaData);
        }
        Collection<Projection> columnProjections =
                encryptColumn.getAssistedQuery().map(optional -> createColumnProjections(optional.getName(), columnSegment.getIdentifier().getQuoteCharacter(), databaseType))
                        .orElseGet(() -> createColumnProjections(encryptColumn.getCipher().getName(), columnSegment.getIdentifier().getQuoteCharacter(), databaseType));
        return new SubstitutableColumnNameToken(startIndex, stopIndex, columnProjections, databaseType, database, metaData);
        
    }
    
    private Collection<Projection> createColumnProjections(final String columnName, final QuoteCharacter quoteCharacter, final DatabaseType databaseType) {
        return Collections.singleton(new ColumnProjection(null, new IdentifierValue(columnName, quoteCharacter), null, databaseType));
    }
    
    private boolean includesLike(final Collection<ExpressionSegment> expressionSegments, final ColumnSegment targetColumnSegment) {
        for (ExpressionSegment each : expressionSegments) {
            Collection<ExpressionSegment> expressions = ExpressionExtractor.extractAllExpressions(each);
            if (isLikeColumnSegment(expressions, targetColumnSegment)) {
                return true;
            }
        }
        return false;
    }
    
    private boolean isLikeColumnSegment(final Collection<ExpressionSegment> expressions, final ColumnSegment targetColumnSegment) {
        for (ExpressionSegment each : expressions) {
            if (each instanceof BinaryOperationExpression
                    && "LIKE".equalsIgnoreCase(((BinaryOperationExpression) each).getOperator()) && isSameColumnSegment(((BinaryOperationExpression) each).getLeft(), targetColumnSegment)) {
                return true;
            }
        }
        return false;
    }
    
    private boolean isSameColumnSegment(final ExpressionSegment columnSegment, final ColumnSegment targetColumnSegment) {
        return columnSegment instanceof ColumnSegment && columnSegment.getStartIndex() == targetColumnSegment.getStartIndex() && columnSegment.getStopIndex() == targetColumnSegment.getStopIndex();
    }
    
    private Collection<SQLToken> generateSQLTokensForSelect(final SelectStatementContext selectStatementContext) {
        if (!containsEncryptTable(rule, selectStatementContext)) {
            return Collections.emptyList();
        }
        final Collection<SQLToken> result = new LinkedList<>();
        EncryptSelectProjectionTokenGenerator projectionTokenGenerator = new EncryptSelectProjectionTokenGenerator(rule, databaseEncryptRules, database, metaData);
        projectionTokenGenerator.setPreviousSQLTokens(previousSQLTokens);
        if (projectionTokenGenerator.isGenerateSQLToken(selectStatementContext)) {
            result.addAll(projectionTokenGenerator.generateSQLTokens(selectStatementContext));
        }
        EncryptPredicateColumnTokenGenerator predicateColumnTokenGenerator = new EncryptPredicateColumnTokenGenerator(rule, databaseEncryptRules, database, metaData);
        if (predicateColumnTokenGenerator.isGenerateSQLToken(selectStatementContext)) {
            result.addAll(predicateColumnTokenGenerator.generateSQLTokens(selectStatementContext));
        }
        EncryptPredicateValueTokenGenerator predicateValueTokenGenerator = new EncryptPredicateValueTokenGenerator(rule, databaseEncryptRules, database);
        predicateValueTokenGenerator.setParameters(parameters);
        Collection<WhereSegment> whereSegments = selectStatementContext.getWhereSegments();
        Collection<EncryptCondition> encryptConditions = new EncryptConditionEngine(rule, databaseEncryptRules).createEncryptConditions(whereSegments);
        predicateValueTokenGenerator.setEncryptConditions(encryptConditions);
        if (predicateValueTokenGenerator.isGenerateSQLToken(selectStatementContext)) {
            result.addAll(predicateValueTokenGenerator.generateSQLTokens(selectStatementContext));
        }
        EncryptOrderByItemTokenGenerator orderByItemTokenGenerator = new EncryptOrderByItemTokenGenerator(rule, database, metaData);
        if (orderByItemTokenGenerator.isGenerateSQLToken(selectStatementContext)) {
            result.addAll(orderByItemTokenGenerator.generateSQLTokens(selectStatementContext));
        }
        EncryptGroupByItemTokenGenerator groupByItemTokenGenerator = new EncryptGroupByItemTokenGenerator(rule, database, metaData);
        if (groupByItemTokenGenerator.isGenerateSQLToken(selectStatementContext)) {
            result.addAll(groupByItemTokenGenerator.generateSQLTokens(selectStatementContext));
        }
        EncryptUsingNaturalJoinTokenGenerator usingNaturalJoinTokenGenerator = new EncryptUsingNaturalJoinTokenGenerator(rule, databaseEncryptRules);
        if (usingNaturalJoinTokenGenerator.isGenerateSQLToken(selectStatementContext)) {
            result.addAll(usingNaturalJoinTokenGenerator.generateSQLTokens(selectStatementContext));
        }
        EncryptSelectForUpdateTokenGenerator selectForUpdateTokenGenerator = new EncryptSelectForUpdateTokenGenerator(rule, database, metaData);
        if (selectForUpdateTokenGenerator.isGenerateSQLToken(selectStatementContext)) {
            result.addAll(selectForUpdateTokenGenerator.generateSQLTokens(selectStatementContext));
        }
        return result;
    }
    
    private boolean containsEncryptTable(final EncryptRule encryptRule, final SelectStatementContext sqlStatementContext) {
        for (String each : sqlStatementContext.getTablesContext().getTableNames()) {
            if (encryptRule.findEncryptTable(each).isPresent()) {
                return true;
            }
        }
        return false;
    }
    
    private Collection<SQLToken> generateSQLTokensForUpdate(final MergeStatementContext mergeStatementContext) {
        UpdateStatement updateStatement = mergeStatementContext.getSqlStatement().getUpdate().orElseThrow(() -> new IllegalStateException("Can not get merge update statement."));
        Collection<SQLToken> result = new LinkedList<>();
        updateStatement.getAssignmentSegment().ifPresent(optional -> result.addAll(generateSQLTokensForAssignments(optional, mergeStatementContext)));
        result.addAll(generateSQLTokensForUpdateWhere(mergeStatementContext));
        return result;
    }
    
    private Collection<SQLToken> generateSQLTokensForAssignments(final SetAssignmentSegment assignmentSegment, final MergeStatementContext mergeStatementContext) {
        String targetTable = ((SimpleTableSegment) mergeStatementContext.getSqlStatement().getTarget()).getTableName().getIdentifier().getValue();
        TableSegment sourceTable = mergeStatementContext.getSqlStatement().getSource();
        String schemaName =
                mergeStatementContext.getTablesContext().getSchemaName().orElseGet(() -> new DatabaseTypeRegistry(mergeStatementContext.getDatabaseType()).getDefaultSchemaName(database.getName()));
        Collection<SQLToken> result = new LinkedList<>();
        for (ColumnAssignmentSegment each : assignmentSegment.getAssignments()) {
            addSQLTokensForAssignment(each, sourceTable, targetTable, schemaName, result);
        }
        return result;
    }
    
    private void addSQLTokensForAssignment(final ColumnAssignmentSegment segment, final TableSegment sourceTable, final String targetTable, final String schemaName,
                                           final Collection<SQLToken> sqlTokens) {
        if (segment.getValue() instanceof ParameterMarkerExpressionSegment && rule.findEncryptTable(targetTable).isPresent()
                && rule.getEncryptTable(targetTable).isEncryptColumn(segment.getColumns().get(0).getIdentifier().getValue())) {
            sqlTokens.add(generateParamterSQLToken(rule.getEncryptTable(targetTable).getEncryptColumn(segment.getColumns().get(0).getIdentifier().getValue()), segment));
        }
        if (segment.getValue() instanceof LiteralExpressionSegment && rule.findEncryptTable(targetTable).isPresent()
                && rule.getEncryptTable(targetTable).isEncryptColumn(segment.getColumns().get(0).getIdentifier().getValue())) {
            sqlTokens.add(generateLiteralSQLToken(schemaName, targetTable, rule.getEncryptTable(targetTable).getEncryptColumn(segment.getColumns().get(0).getIdentifier().getValue()), segment));
        }
        if (segment.getValue() instanceof ColumnSegment) {
            addSQLTokensForAssignColumn(segment, sourceTable, targetTable, sqlTokens);
        }
    }
    
    private SQLToken generateParamterSQLToken(final EncryptColumn encryptColumn, final ColumnAssignmentSegment segment) {
        EncryptParameterAssignmentToken result =
                new EncryptParameterAssignmentToken(segment.getColumns().get(0).getStartIndex(), segment.getStopIndex(), segment.getColumns().get(0).getIdentifier().getQuoteCharacter(), null);
        result.addColumnName(encryptColumn.getCipher().getName());
        encryptColumn.getAssistedQuery().ifPresent(optional -> result.addColumnName(optional.getName()));
        encryptColumn.getLikeQuery().ifPresent(optional -> result.addColumnName(optional.getName()));
        encryptColumn.getOrderQuery().ifPresent(optional -> result.addColumnName(optional.getName()));
        encryptColumn.getPlain().ifPresent(optional -> result.addColumnName(optional.getName()));
        return result;
    }
    
    private EncryptAssignmentToken generateLiteralSQLToken(final String schemaName, final String tableName, final EncryptColumn encryptColumn, final ColumnAssignmentSegment segment) {
        @SphereEx(Type.MODIFY)
        EncryptLiteralAssignmentToken result =
                new EncryptLiteralAssignmentToken(segment.getColumns().get(0).getStartIndex(), segment.getStopIndex(), segment.getColumns().get(0).getIdentifier().getQuoteCharacter(), null);
        addCipherAssignment(schemaName, tableName, encryptColumn, segment, result);
        addAssistedQueryAssignment(schemaName, tableName, encryptColumn, segment, result);
        addLikeAssignment(schemaName, tableName, encryptColumn, segment, result);
        addOrderAssignment(schemaName, tableName, encryptColumn, segment, result);
        addPlainAssignment(encryptColumn, segment, result);
        return result;
    }
    
    private void addCipherAssignment(final String schemaName, final String tableName,
                                     final EncryptColumn encryptColumn, final ColumnAssignmentSegment segment, final EncryptLiteralAssignmentToken token) {
        Object originalValue = ((LiteralExpressionSegment) segment.getValue()).getLiterals();
        Object cipherValue = encryptColumn.getCipher().encrypt(database.getName(), schemaName, tableName, encryptColumn.getName(), Collections.singletonList(originalValue)).iterator().next();
        token.addAssignment(encryptColumn.getCipher().getName(), cipherValue);
    }
    
    private void addAssistedQueryAssignment(final String schemaName, final String tableName, final EncryptColumn encryptColumn,
                                            final ColumnAssignmentSegment segment, final EncryptLiteralAssignmentToken token) {
        Object originalValue = ((LiteralExpressionSegment) segment.getValue()).getLiterals();
        if (encryptColumn.getAssistedQuery().isPresent()) {
            Object assistedQueryValue = encryptColumn.getAssistedQuery().get().encrypt(
                    database.getName(), schemaName, tableName, encryptColumn.getName(), Collections.singletonList(originalValue)).iterator().next();
            token.addAssignment(encryptColumn.getAssistedQuery().get().getName(), assistedQueryValue);
        }
    }
    
    private void addLikeAssignment(final String schemaName, final String tableName, final EncryptColumn encryptColumn, final ColumnAssignmentSegment segment,
                                   final EncryptLiteralAssignmentToken token) {
        Object originalValue = ((LiteralExpressionSegment) segment.getValue()).getLiterals();
        if (encryptColumn.getLikeQuery().isPresent()) {
            Object assistedQueryValue = encryptColumn.getLikeQuery().get().encrypt(database.getName(), schemaName,
                    tableName, segment.getColumns().get(0).getIdentifier().getValue(), Collections.singletonList(originalValue)).iterator().next();
            token.addAssignment(encryptColumn.getLikeQuery().get().getName(), assistedQueryValue);
        }
    }
    
    private void addOrderAssignment(final String schemaName, final String tableName, final EncryptColumn encryptColumn, final ColumnAssignmentSegment segment,
                                    final EncryptLiteralAssignmentToken token) {
        Object originalValue = ((LiteralExpressionSegment) segment.getValue()).getLiterals();
        if (encryptColumn.getOrderQuery().isPresent()) {
            Object assistedQueryValue = encryptColumn.getOrderQuery().get().encrypt(database.getName(), schemaName,
                    tableName, segment.getColumns().get(0).getIdentifier().getValue(), Collections.singletonList(originalValue)).iterator().next();
            token.addAssignment(encryptColumn.getOrderQuery().get().getName(), assistedQueryValue);
        }
    }
    
    private void addPlainAssignment(final EncryptColumn encryptColumn, final ColumnAssignmentSegment segment, final EncryptLiteralAssignmentToken token) {
        Object originalValue = ((LiteralExpressionSegment) segment.getValue()).getLiterals();
        encryptColumn.getPlain().ifPresent(optional -> token.addAssignment(optional.getName(), originalValue));
    }
    
    private void addSQLTokensForAssignColumn(final ColumnAssignmentSegment segment, final TableSegment sourceTable, final String targetTable, final Collection<SQLToken> sqlTokens) {
        ColumnSegment rightColumn = (ColumnSegment) segment.getValue();
        ColumnSegment leftColumn = segment.getColumns().get(0);
        if (rule.findEncryptTable(targetTable).isPresent() && rule.getEncryptTable(targetTable).isEncryptColumn(leftColumn.getIdentifier().getValue())) {
            if (sourceTable instanceof SimpleTableSegment && rule.findEncryptTable(((SimpleTableSegment) sourceTable).getTableName().getIdentifier().getValue()).isPresent()
                    && rule.getEncryptTable(((SimpleTableSegment) sourceTable).getTableName().getIdentifier().getValue()).isEncryptColumn(rightColumn.getIdentifier().getValue())) {
                ShardingSpherePreconditions.checkState(isSameColumnEncrypt(rule.getEncryptTable(targetTable).getEncryptColumn(leftColumn.getIdentifier().getValue()),
                        rule.getEncryptTable(((SimpleTableSegment) sourceTable).getTableName().getIdentifier().getValue()).getEncryptColumn(rightColumn.getIdentifier().getValue())),
                        () -> new UnsupportedSQLOperationException(
                                "Can not use different encryptor for " + leftColumn.getColumnBoundInfo() + " and " + rightColumn.getColumnBoundInfo() + " in update set clause"));
                sqlTokens.add(generateColumnSQLToken(segment, leftColumn, rightColumn, rule.getEncryptTable(targetTable).getEncryptColumn(leftColumn.getIdentifier().getValue()),
                        rule.getEncryptTable(((SimpleTableSegment) sourceTable).getTableName().getIdentifier().getValue()).getEncryptColumn(rightColumn.getIdentifier().getValue())));
            } else {
                throw new UnsupportedSQLOperationException(
                        "Can not use different encryptor for " + leftColumn.getColumnBoundInfo() + " and " + rightColumn.getColumnBoundInfo() + " in update set clause");
            }
        } else if (sourceTable instanceof SimpleTableSegment && rule.findEncryptTable(((SimpleTableSegment) sourceTable).getTableName().getIdentifier().getValue()).isPresent()
                && rule.getEncryptTable(((SimpleTableSegment) sourceTable).getTableName().getIdentifier().getValue()).isEncryptColumn(rightColumn.getIdentifier().getValue())) {
            throw new UnsupportedSQLOperationException(
                    "Can not use different encryptor for " + leftColumn.getColumnBoundInfo() + " and " + rightColumn.getColumnBoundInfo() + " in update set clause");
        }
    }
    
    private boolean isSameColumnEncrypt(final EncryptColumn leftEncryptColumn, final EncryptColumn rightEncryptColumn) {
        if (leftEncryptColumn.getPlain().isPresent() && rightEncryptColumn.getPlain().isPresent() || !leftEncryptColumn.getPlain().isPresent() && !rightEncryptColumn.getPlain().isPresent()) {
            return leftEncryptColumn.getAssistedQuery().map(AssistedQueryColumnItem::getEncryptor).equals(rightEncryptColumn.getAssistedQuery().map(AssistedQueryColumnItem::getEncryptor))
                    && leftEncryptColumn.getLikeQuery().map(LikeQueryColumnItem::getEncryptor).equals(rightEncryptColumn.getLikeQuery().map(LikeQueryColumnItem::getEncryptor))
                    && leftEncryptColumn.getOrderQuery().map(OrderQueryColumnItem::getEncryptor).equals(rightEncryptColumn.getOrderQuery().map(OrderQueryColumnItem::getEncryptor))
                    && leftEncryptColumn.getCipher().getEncryptor().equals(rightEncryptColumn.getCipher().getEncryptor());
        }
        return false;
    }
    
    @SuppressWarnings("OptionalGetWithoutIsPresent")
    private SQLToken generateColumnSQLToken(final ColumnAssignmentSegment segment, final ColumnSegment leftColumn, final ColumnSegment rightColumn,
                                            final EncryptColumn leftEncryptColumn, final EncryptColumn rightEncryptColumn) {
        EncryptColumnAssignmentToken result =
                new EncryptColumnAssignmentToken(segment.getColumns().get(0).getStartIndex(), segment.getStopIndex(), segment.getColumns().get(0).getIdentifier().getQuoteCharacter());
        IdentifierValue leftOwner = leftColumn.getOwner().map(OwnerSegment::getIdentifier).orElse(null);
        IdentifierValue rightOwner = rightColumn.getOwner().map(OwnerSegment::getIdentifier).orElse(null);
        QuoteCharacter leftQuote = leftColumn.getIdentifier().getQuoteCharacter();
        QuoteCharacter rightQuote = rightColumn.getIdentifier().getQuoteCharacter();
        result.addAssignment(leftOwner, new IdentifierValue(leftEncryptColumn.getCipher().getName(), leftQuote), rightOwner,
                new IdentifierValue(rightEncryptColumn.getCipher().getName(), rightQuote));
        leftEncryptColumn.getAssistedQuery().ifPresent(optional -> result.addAssignment(leftOwner,
                new IdentifierValue(optional.getName(), leftQuote), rightOwner, new IdentifierValue(rightEncryptColumn.getAssistedQuery().get().getName(), rightQuote)));
        leftEncryptColumn.getLikeQuery().ifPresent(optional -> result.addAssignment(leftOwner,
                new IdentifierValue(optional.getName(), leftQuote), rightOwner, new IdentifierValue(rightEncryptColumn.getLikeQuery().get().getName(), rightQuote)));
        leftEncryptColumn.getOrderQuery().ifPresent(optional -> result.addAssignment(leftOwner,
                new IdentifierValue(optional.getName(), leftQuote), rightOwner, new IdentifierValue(rightEncryptColumn.getOrderQuery().get().getName(), rightQuote)));
        leftEncryptColumn.getPlain().ifPresent(optional -> result.addAssignment(leftOwner,
                new IdentifierValue(optional.getName(), leftQuote), rightOwner, new IdentifierValue(rightEncryptColumn.getPlain().get().getName(), rightQuote)));
        return result;
    }
    
    private Collection<SQLToken> generateSQLTokensForUpdateWhere(final MergeStatementContext mergeStatementContext) {
        MergeStatement mergeStatement = mergeStatementContext.getSqlStatement();
        Collection<WhereSegment> whereSegments = new LinkedList<>();
        UpdateStatement updateStatement = mergeStatement.getUpdate().orElseThrow(() -> new IllegalStateException("Can not get merge updateStatement statement."));
        updateStatement.getWhere().ifPresent(whereSegments::add);
        if (updateStatement instanceof OracleUpdateStatement && updateStatement.getDeleteWhere().isPresent()) {
            whereSegments.add(updateStatement.getDeleteWhere().get());
        }
        return generateForWhereSegments(whereSegments, mergeStatementContext);
    }
    
    private Collection<SQLToken> generateForWhereSegments(final Collection<WhereSegment> whereSegments, final MergeStatementContext mergeStatementContext) {
        Collection<SQLToken> result = new LinkedList<>();
        for (WhereSegment each : whereSegments) {
            Collection<BinaryOperationExpression> joinConditions = new LinkedList<>();
            ExpressionExtractor.extractJoinConditions(joinConditions, Collections.singleton(each));
            JoinConditionsEncryptorChecker.checkIsSame(joinConditions, rule, databaseEncryptRules, "where clause with two equal column");
            Collection<ColumnSegment> columnSegments = ColumnExtractor.extract(each.getExpr());
            result.addAll(generateSQLTokensForPredicateColumns(columnSegments, Collections.singleton(each.getExpr()), mergeStatementContext.getDatabaseType()));
            result.addAll(generateSQLTokensForPredicateRightValue(each.getExpr(), columnSegments, mergeStatementContext));
        }
        return result;
    }
    
    private Collection<SQLToken> generateSQLTokensForInsert(final MergeStatementContext mergeStatementContext) {
        Collection<SQLToken> result = new LinkedList<>();
        InsertStatementContext insertStatementContext = mergeStatementContext.getInsertStatementContext();
        String insertTable = ((SimpleTableSegment) mergeStatementContext.getSqlStatement().getTarget()).getTableName().getIdentifier().getValue();
        if (rule.findEncryptTable(insertTable).isPresent() && insertStatementContext.getSqlStatement().getInsertColumns().isPresent()
                && !insertStatementContext.getSqlStatement().getInsertColumns().get().getColumns().isEmpty()) {
            EncryptInsertCipherNameTokenGenerator cipherNameTokenGenerator = new EncryptInsertCipherNameTokenGenerator(rule, databaseEncryptRules, database, metaData);
            result.addAll(cipherNameTokenGenerator.generateSQLTokens(insertStatementContext));
            EncryptInsertDerivedColumnsTokenGenerator derivedColumnsTokenGenerator = new EncryptInsertDerivedColumnsTokenGenerator(rule);
            result.addAll(derivedColumnsTokenGenerator.generateSQLTokens(insertStatementContext));
        }
        if (rule.findEncryptTable(insertTable).isPresent() && !insertStatementContext.containsInsertColumns()) {
            EncryptInsertDefaultColumnsTokenGenerator defaultColumnsTokenGenerator = new EncryptInsertDefaultColumnsTokenGenerator(rule, databaseEncryptRules);
            defaultColumnsTokenGenerator.setPreviousSQLTokens(previousSQLTokens);
            result.add(defaultColumnsTokenGenerator.generateSQLToken(insertStatementContext));
        }
        addSQLTokensForInsertValues(insertStatementContext.getSqlStatement().getValues(), mergeStatementContext, result);
        InsertStatement insertStatement = mergeStatementContext.getSqlStatement().getInsert().orElseThrow(() -> new IllegalStateException("Can not get merge insert statement."));
        insertStatement.getWhere().ifPresent(optional -> result.addAll(generateForWhereSegments(Collections.singleton(optional), mergeStatementContext)));
        return result;
    }
    
    private void addSQLTokensForInsertValues(final Collection<InsertValuesSegment> values, final MergeStatementContext mergeStatementContext, final Collection<SQLToken> sqlTokens) {
        Optional<EncryptTable> targetTableRule = rule.findEncryptTable(((SimpleTableSegment) mergeStatementContext.getSqlStatement().getTarget()).getTableName().getIdentifier().getValue());
        if (!targetTableRule.isPresent()) {
            return;
        }
        String schemaName = mergeStatementContext.getInsertStatementContext().getTablesContext().getSchemaName()
                .orElseGet(() -> new DatabaseTypeRegistry(mergeStatementContext.getInsertStatementContext().getDatabaseType()).getDefaultSchemaName(database.getName()));
        InsertValuesToken token = new EncryptInsertValuesToken(getStartIndex(values), getStopIndex(values));
        for (InsertValueContext each : mergeStatementContext.getInsertStatementContext().getInsertValueContexts()) {
            InsertValue insertValueToken = new InsertValue(new ArrayList<>(each.getValueExpressions()));
            encryptToken(insertValueToken, schemaName, targetTableRule.get(), mergeStatementContext.getInsertStatementContext(), each);
            token.getInsertValues().add(insertValueToken);
        }
        sqlTokens.add(token);
    }
    
    private int getStartIndex(final Collection<InsertValuesSegment> segments) {
        int result = segments.iterator().next().getStartIndex();
        for (InsertValuesSegment each : segments) {
            result = Math.min(result, each.getStartIndex());
        }
        return result;
    }
    
    private int getStopIndex(final Collection<InsertValuesSegment> segments) {
        int result = segments.iterator().next().getStopIndex();
        for (InsertValuesSegment each : segments) {
            result = Math.max(result, each.getStopIndex());
        }
        return result;
    }
    
    private void encryptToken(final InsertValue insertValueToken, final String schemaName, final EncryptTable encryptTable,
                              final InsertStatementContext insertStatementContext, final InsertValueContext insertValueContext) {
        String tableName = encryptTable.getTable();
        Iterator<String> descendingColumnNames = insertStatementContext.getDescendingColumnNames();
        while (descendingColumnNames.hasNext()) {
            String columnName = descendingColumnNames.next();
            if (!encryptTable.isEncryptColumn(columnName)) {
                continue;
            }
            EncryptColumn encryptColumn = encryptTable.getEncryptColumn(columnName);
            int columnIndex = insertStatementContext.getColumnNames().indexOf(columnName);
            ExpressionSegment valueExpression = insertValueContext.getValueExpressions().get(columnIndex);
            setCipherColumn(schemaName, tableName, encryptColumn, insertValueToken, valueExpression, columnIndex);
            int indexDelta = 1;
            if (encryptColumn.getAssistedQuery().isPresent()) {
                addAssistedQueryColumn(schemaName, tableName, encryptColumn, insertValueContext, insertValueToken, columnIndex, indexDelta);
                indexDelta++;
            }
            if (encryptColumn.getLikeQuery().isPresent()) {
                addLikeQueryColumn(schemaName, tableName, encryptColumn, insertValueContext, insertValueToken, columnIndex, indexDelta);
                indexDelta++;
            }
            if (encryptColumn.getOrderQuery().isPresent()) {
                addOrderQueryColumn(schemaName, tableName, encryptColumn, insertValueContext, insertValueToken, columnIndex, indexDelta);
                indexDelta++;
            }
            if (encryptColumn.getPlain().isPresent()) {
                addPlainColumn(insertValueContext, insertValueToken, columnIndex, indexDelta);
            }
        }
    }
    
    private void setCipherColumn(final String schemaName, final String tableName, final EncryptColumn encryptColumn,
                                 final InsertValue insertValueToken, final ExpressionSegment valueExpression, final int columnIndex) {
        if (valueExpression instanceof LiteralExpressionSegment) {
            insertValueToken.getValues().set(columnIndex, new LiteralExpressionSegment(valueExpression.getStartIndex(), valueExpression.getStopIndex(),
                    encryptColumn.getCipher().encrypt(database.getName(), schemaName, tableName, encryptColumn.getName(), ((LiteralExpressionSegment) valueExpression).getLiterals())));
        }
        if (valueExpression instanceof ColumnSegment) {
            IdentifierValue identifierValue = ((ColumnSegment) valueExpression).getColumnBoundInfo().getOriginalTable();
            if (null == identifierValue || !rule.findEncryptTable(identifierValue.getValue()).isPresent()
                    || !rule.getEncryptTable(identifierValue.getValue()).isEncryptColumn(((ColumnSegment) valueExpression).getIdentifier().getValue())
                    || !isSameColumnEncrypt(encryptColumn, rule.getEncryptTable(identifierValue.getValue()).getEncryptColumn(((ColumnSegment) valueExpression).getIdentifier().getValue()))) {
                throw new UnsupportedSQLOperationException("Can not use different encryptor in insert value clause");
            }
            ColumnSegment columnSegment = new ColumnSegment(valueExpression.getStartIndex(), valueExpression.getStopIndex(),
                    new IdentifierValue(rule.getEncryptTable(identifierValue.getValue()).getEncryptColumn(((ColumnSegment) valueExpression).getIdentifier().getValue()).getCipher().getName(),
                            ((ColumnSegment) valueExpression).getIdentifier().getQuoteCharacter()));
            ((ColumnSegment) valueExpression).getOwner().ifPresent(columnSegment::setOwner);
            insertValueToken.getValues().set(columnIndex, columnSegment);
        }
    }
    
    @SuppressWarnings("OptionalGetWithoutIsPresent")
    private void addAssistedQueryColumn(final String schemaName, final String tableName, final EncryptColumn encryptColumn,
                                        final InsertValueContext insertValueContext, final InsertValue insertValueToken, final int columnIndex, final int indexDelta) {
        Optional<AssistedQueryColumnItem> assistedQueryColumnItem = encryptColumn.getAssistedQuery();
        Preconditions.checkState(assistedQueryColumnItem.isPresent());
        ExpressionSegment originalValue = insertValueContext.getValueExpressions().get(columnIndex);
        ExpressionSegment derivedValue = null;
        if (originalValue instanceof LiteralExpressionSegment) {
            derivedValue = new DerivedLiteralExpressionSegment(
                    assistedQueryColumnItem.get().encrypt(database.getName(), schemaName, tableName, encryptColumn.getName(), ((LiteralExpressionSegment) originalValue).getLiterals()));
        }
        if (originalValue instanceof ParameterMarkerExpressionSegment) {
            derivedValue = new DerivedParameterMarkerExpressionSegment(getParameterIndexCount(insertValueToken));
        }
        if (originalValue instanceof ColumnSegment) {
            derivedValue = new ColumnSegment(originalValue.getStartIndex(), originalValue.getStopIndex(), new IdentifierValue(rule.getEncryptTable(((ColumnSegment) originalValue)
                    .getColumnBoundInfo().getOriginalTable().getValue()).getEncryptColumn(((ColumnSegment) originalValue).getIdentifier().getValue()).getAssistedQuery().get().getName(),
                    ((ColumnSegment) originalValue).getIdentifier().getQuoteCharacter()));
            ((ColumnSegment) originalValue).getOwner().ifPresent(((ColumnSegment) derivedValue)::setOwner);
        }
        if (null != derivedValue) {
            insertValueToken.getValues().add(columnIndex + indexDelta, derivedValue);
        }
    }
    
    private int getParameterIndexCount(final InsertValue insertValueToken) {
        int result = 0;
        for (ExpressionSegment each : insertValueToken.getValues()) {
            if (each instanceof ParameterMarkerExpressionSegment) {
                result++;
            }
        }
        return result;
    }
    
    @SuppressWarnings("OptionalGetWithoutIsPresent")
    private void addLikeQueryColumn(final String schemaName, final String tableName, final EncryptColumn encryptColumn,
                                    final InsertValueContext insertValueContext, final InsertValue insertValueToken, final int columnIndex, final int indexDelta) {
        Optional<LikeQueryColumnItem> likeQueryColumnItem = encryptColumn.getLikeQuery();
        Preconditions.checkState(likeQueryColumnItem.isPresent());
        ExpressionSegment originalValue = insertValueContext.getValueExpressions().get(columnIndex);
        ExpressionSegment derivedValue = null;
        if (originalValue instanceof LiteralExpressionSegment) {
            derivedValue = new DerivedLiteralExpressionSegment(
                    likeQueryColumnItem.get().encrypt(database.getName(), schemaName, tableName, encryptColumn.getName(), ((LiteralExpressionSegment) originalValue).getLiterals()));
        }
        if (originalValue instanceof ParameterMarkerExpressionSegment) {
            derivedValue = new DerivedParameterMarkerExpressionSegment(getParameterIndexCount(insertValueToken));
        }
        if (originalValue instanceof ColumnSegment) {
            derivedValue = new ColumnSegment(originalValue.getStartIndex(), originalValue.getStopIndex(), new IdentifierValue(rule.getEncryptTable(((ColumnSegment) originalValue)
                    .getColumnBoundInfo().getOriginalTable().getValue()).getEncryptColumn(((ColumnSegment) originalValue).getIdentifier().getValue()).getLikeQuery().get().getName(),
                    ((ColumnSegment) originalValue).getIdentifier().getQuoteCharacter()));
            ((ColumnSegment) originalValue).getOwner().ifPresent(((ColumnSegment) derivedValue)::setOwner);
        }
        if (null != derivedValue) {
            insertValueToken.getValues().add(columnIndex + indexDelta, derivedValue);
        }
    }
    
    @SuppressWarnings("OptionalGetWithoutIsPresent")
    private void addOrderQueryColumn(final String schemaName, final String tableName, final EncryptColumn encryptColumn,
                                     final InsertValueContext insertValueContext, final InsertValue insertValueToken, final int columnIndex, final int indexDelta) {
        Optional<OrderQueryColumnItem> orderQueryColumnItem = encryptColumn.getOrderQuery();
        Preconditions.checkState(orderQueryColumnItem.isPresent());
        ExpressionSegment originalValue = insertValueContext.getValueExpressions().get(columnIndex);
        ExpressionSegment derivedValue = null;
        if (originalValue instanceof LiteralExpressionSegment) {
            derivedValue = new DerivedLiteralExpressionSegment(
                    orderQueryColumnItem.get().encrypt(database.getName(), schemaName, tableName, encryptColumn.getName(), ((LiteralExpressionSegment) originalValue).getLiterals()));
        }
        if (originalValue instanceof ParameterMarkerExpressionSegment) {
            derivedValue = new DerivedParameterMarkerExpressionSegment(getParameterIndexCount(insertValueToken));
        }
        if (originalValue instanceof ColumnSegment) {
            derivedValue = new ColumnSegment(originalValue.getStartIndex(), originalValue.getStopIndex(), new IdentifierValue(rule.getEncryptTable(((ColumnSegment) originalValue)
                    .getColumnBoundInfo().getOriginalTable().getValue()).getEncryptColumn(((ColumnSegment) originalValue).getIdentifier().getValue()).getOrderQuery().get().getName(),
                    ((ColumnSegment) originalValue).getIdentifier().getQuoteCharacter()));
            ((ColumnSegment) originalValue).getOwner().ifPresent(((ColumnSegment) derivedValue)::setOwner);
        }
        if (null != derivedValue) {
            insertValueToken.getValues().add(columnIndex + indexDelta, derivedValue);
        }
    }
    
    @SuppressWarnings("OptionalGetWithoutIsPresent")
    private void addPlainColumn(final InsertValueContext insertValueContext, final InsertValue insertValueToken, final int columnIndex, final int indexDelta) {
        ExpressionSegment originalValue = insertValueContext.getValueExpressions().get(columnIndex);
        ExpressionSegment derivedValue = null;
        if (originalValue instanceof LiteralExpressionSegment) {
            derivedValue = new DerivedLiteralExpressionSegment(((LiteralExpressionSegment) originalValue).getLiterals());
        }
        if (originalValue instanceof ParameterMarkerExpressionSegment) {
            derivedValue = new DerivedParameterMarkerExpressionSegment(getParameterIndexCount(insertValueToken));
        }
        if (originalValue instanceof ColumnSegment) {
            derivedValue = new ColumnSegment(originalValue.getStartIndex(), originalValue.getStopIndex(), new IdentifierValue(rule.getEncryptTable(((ColumnSegment) originalValue)
                    .getColumnBoundInfo().getOriginalTable().getValue()).getEncryptColumn(((ColumnSegment) originalValue).getIdentifier().getValue()).getPlain().get().getName(),
                    ((ColumnSegment) originalValue).getIdentifier().getQuoteCharacter()));
            ((ColumnSegment) originalValue).getOwner().ifPresent(((ColumnSegment) derivedValue)::setOwner);
        }
        if (null != derivedValue) {
            insertValueToken.getValues().add(columnIndex + indexDelta, derivedValue);
        }
    }
}
