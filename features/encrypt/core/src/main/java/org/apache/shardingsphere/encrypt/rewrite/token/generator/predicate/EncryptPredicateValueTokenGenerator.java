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

package org.apache.shardingsphere.encrypt.rewrite.token.generator.predicate;

import com.sphereex.dbplusengine.SphereEx;
import com.sphereex.dbplusengine.SphereEx.Type;
import com.sphereex.dbplusengine.encrypt.rewrite.condition.impl.EncryptBetweenCondition;
import com.sphereex.dbplusengine.encrypt.rewrite.token.cryptographic.pojo.EncryptBetweenValueToken;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import org.apache.shardingsphere.encrypt.exception.metadata.MissingMatchedEncryptQueryAlgorithmException;
import org.apache.shardingsphere.encrypt.rewrite.aware.EncryptConditionsAware;
import org.apache.shardingsphere.encrypt.rewrite.condition.EncryptCondition;
import org.apache.shardingsphere.encrypt.rewrite.condition.EncryptConditionValues;
import org.apache.shardingsphere.encrypt.rewrite.condition.impl.EncryptBinaryCondition;
import org.apache.shardingsphere.encrypt.rewrite.condition.impl.EncryptInCondition;
import org.apache.shardingsphere.encrypt.rewrite.token.pojo.EncryptPredicateEqualRightValueToken;
import org.apache.shardingsphere.encrypt.rewrite.token.pojo.EncryptPredicateFunctionRightValueToken;
import org.apache.shardingsphere.encrypt.rewrite.token.pojo.EncryptPredicateInRightValueToken;
import org.apache.shardingsphere.encrypt.rule.EncryptRule;
import org.apache.shardingsphere.encrypt.rule.column.EncryptColumn;
import org.apache.shardingsphere.encrypt.rule.column.item.LikeQueryColumnItem;
import org.apache.shardingsphere.encrypt.rule.table.EncryptTable;
import org.apache.shardingsphere.infra.annotation.HighFrequencyInvocation;
import org.apache.shardingsphere.infra.binder.context.statement.SQLStatementContext;
import org.apache.shardingsphere.infra.binder.context.type.TableAvailable;
import org.apache.shardingsphere.infra.binder.context.type.WhereAvailable;
import org.apache.shardingsphere.infra.database.core.type.DatabaseTypeRegistry;
import org.apache.shardingsphere.infra.exception.core.ShardingSpherePreconditions;
import org.apache.shardingsphere.infra.metadata.database.ShardingSphereDatabase;
import org.apache.shardingsphere.infra.rewrite.sql.token.common.generator.CollectionSQLTokenGenerator;
import org.apache.shardingsphere.infra.rewrite.sql.token.common.generator.aware.ParametersAware;
import org.apache.shardingsphere.infra.rewrite.sql.token.common.pojo.SQLToken;
import org.apache.shardingsphere.sql.parser.statement.core.segment.dml.expr.FunctionSegment;
import org.apache.shardingsphere.sql.parser.statement.core.segment.generic.bound.ColumnSegmentBoundInfo;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Predicate value token generator for encrypt.
 */
@HighFrequencyInvocation
@RequiredArgsConstructor
@Setter
public final class EncryptPredicateValueTokenGenerator implements CollectionSQLTokenGenerator<SQLStatementContext>, ParametersAware, EncryptConditionsAware {
    
    @SphereEx
    private static final Collection<String> GREATER_LESS_COMPARISON_OPERATORS = new HashSet<>(Arrays.asList(">", "<", ">=", "<="));
    
    private final EncryptRule rule;
    
    @SphereEx
    private final Map<String, EncryptRule> databaseEncryptRules;
    
    private final ShardingSphereDatabase database;
    
    private List<Object> parameters;
    
    private Collection<EncryptCondition> encryptConditions;
    
    @Override
    public boolean isGenerateSQLToken(final SQLStatementContext sqlStatementContext) {
        return sqlStatementContext instanceof WhereAvailable;
    }
    
    @Override
    public Collection<SQLToken> generateSQLTokens(final SQLStatementContext sqlStatementContext) {
        Collection<SQLToken> result = new LinkedHashSet<>(encryptConditions.size(), 1F);
        String schemaName = ((TableAvailable) sqlStatementContext).getTablesContext().getSchemaName()
                .orElseGet(() -> new DatabaseTypeRegistry(sqlStatementContext.getDatabaseType()).getDefaultSchemaName(database.getName()));
        for (EncryptCondition each : encryptConditions) {
            // SPEX ADDED: BEGIN
            ColumnSegmentBoundInfo columnBoundInfo = each.getColumnSegment().getColumnBoundInfo();
            String tableName = columnBoundInfo.getOriginalTable().getValue();
            String databaseName = columnBoundInfo.getOriginalDatabase().getValue().isEmpty() ? database.getName() : columnBoundInfo.getOriginalDatabase().getValue();
            Optional<EncryptTable> databaseEncryptTable = Optional.ofNullable(databaseEncryptRules.get(databaseName)).flatMap(rule -> rule.findEncryptTable(tableName));
            // SPEX ADDED: END
            @SphereEx(Type.MODIFY)
            Optional<EncryptTable> encryptTable = Optional.ofNullable(databaseEncryptTable.orElseGet(() -> rule.findEncryptTable(each.getTableName()).orElse(null)));
            encryptTable.ifPresent(optional -> result.add(generateSQLToken(schemaName, optional, each)));
        }
        return result;
    }
    
    private SQLToken generateSQLToken(final String schemaName, final EncryptTable encryptTable, final EncryptCondition encryptCondition) {
        List<Object> originalValues = new EncryptConditionValues(encryptCondition).get(parameters);
        // SPEX ADDED: BEGIN
        if (rule.isQueryWithPlain(encryptCondition.getTableName(), encryptCondition.getColumnSegment().getIdentifier().getValue())) {
            return generateSQLTokenForPlainColumn(schemaName, encryptTable, encryptCondition, originalValues);
        }
        // SPEX ADDED: END
        int startIndex = encryptCondition.getStartIndex();
        int stopIndex = encryptCondition.getStopIndex();
        Map<Integer, Object> indexValues = getPositionValues(
                encryptCondition.getPositionValueMap().keySet(), getEncryptedValues(schemaName, encryptTable, encryptCondition, new EncryptConditionValues(encryptCondition).get(parameters)));
        Collection<Integer> parameterMarkerIndexes = encryptCondition.getPositionIndexMap().keySet();
        if (encryptCondition instanceof EncryptBinaryCondition && ((EncryptBinaryCondition) encryptCondition).getExpressionSegment() instanceof FunctionSegment) {
            FunctionSegment functionSegment = (FunctionSegment) ((EncryptBinaryCondition) encryptCondition).getExpressionSegment();
            return new EncryptPredicateFunctionRightValueToken(startIndex, stopIndex, functionSegment.getFunctionName(), functionSegment.getParameters(), indexValues, parameterMarkerIndexes);
        }
        // SPEX ADDED: BEGIN
        if (encryptCondition instanceof EncryptBetweenCondition && (!indexValues.isEmpty() || !parameterMarkerIndexes.isEmpty())) {
            return new EncryptBetweenValueToken(startIndex, stopIndex, indexValues, parameterMarkerIndexes);
        }
        // SPEX ADDED: END
        return encryptCondition instanceof EncryptInCondition
                ? new EncryptPredicateInRightValueToken(startIndex, stopIndex, indexValues, parameterMarkerIndexes)
                : new EncryptPredicateEqualRightValueToken(startIndex, stopIndex, indexValues, parameterMarkerIndexes);
    }
    
    private List<Object> getEncryptedValues(final String schemaName, final EncryptTable encryptTable, final EncryptCondition encryptCondition, final List<Object> originalValues) {
        String columnName = encryptCondition.getColumnSegment().getIdentifier().getValue();
        EncryptColumn encryptColumn = encryptTable.getEncryptColumn(columnName);
        if (encryptCondition instanceof EncryptBinaryCondition && "LIKE".equalsIgnoreCase(((EncryptBinaryCondition) encryptCondition).getOperator())) {
            LikeQueryColumnItem likeQueryColumnItem = encryptColumn.getLikeQuery()
                    .orElseThrow(() -> new MissingMatchedEncryptQueryAlgorithmException(encryptTable.getTable(), columnName, "LIKE"));
            return likeQueryColumnItem.encrypt(database.getName(), schemaName, encryptCondition.getTableName(), columnName, originalValues);
        }
        String tableName = encryptCondition.getColumnSegment().getColumnBoundInfo().getOriginalTable().getValue();
        // SPEX ADDED: BEGIN
        if (encryptCondition instanceof EncryptBinaryCondition && GREATER_LESS_COMPARISON_OPERATORS.contains(((EncryptBinaryCondition) encryptCondition).getOperator())
                || encryptCondition instanceof EncryptBetweenCondition) {
            ShardingSpherePreconditions.checkState(encryptColumn.getOrderQuery().isPresent() || encryptColumn.getCipher().getEncryptor().getMetaData().isSupportOrder(),
                    () -> new MissingMatchedEncryptQueryAlgorithmException(tableName, encryptCondition.getColumnSegment().getIdentifier().getValue(), "ORDER"));
            return encryptColumn.getOrderQuery()
                    .map(orderQueryColumnItem -> orderQueryColumnItem.encrypt(database.getName(), schemaName, tableName, columnName,
                            originalValues))
                    .orElseGet(() -> encryptColumn.getCipher().encrypt(database.getName(), schemaName, tableName, columnName, originalValues));
        }
        // SPEX ADDED: END
        return encryptColumn.getAssistedQuery()
                .map(optional -> optional.encrypt(database.getName(), schemaName, tableName, encryptCondition.getColumnSegment().getIdentifier().getValue(), originalValues))
                .orElseGet(() -> encryptColumn.getCipher().encrypt(database.getName(), schemaName, tableName, encryptCondition.getColumnSegment().getIdentifier().getValue(), originalValues));
    }
    
    @SphereEx
    private SQLToken generateSQLTokenForPlainColumn(final String schemaName, final EncryptTable encryptTable, final EncryptCondition encryptCondition, final List<Object> originalValues) {
        int startIndex = encryptCondition.getStartIndex();
        int stopIndex = encryptCondition.getStopIndex();
        String columnName = encryptCondition.getColumnSegment().getIdentifier().getValue();
        Map<Integer, Object> indexValues = new HashMap<>(encryptTable.getEncryptColumn(columnName).getPlain().isPresent()
                ? getPositionValues(encryptCondition.getPositionValueMap().keySet(), originalValues)
                : getPositionValues(encryptCondition.getPositionValueMap().keySet(), getEncryptedValues(schemaName, encryptTable, encryptCondition, originalValues)));
        Collection<Integer> parameterMarkerIndexes = encryptCondition.getPositionIndexMap().keySet();
        if (encryptCondition instanceof EncryptBinaryCondition && ((EncryptBinaryCondition) encryptCondition).getExpressionSegment() instanceof FunctionSegment) {
            FunctionSegment functionSegment = (FunctionSegment) ((EncryptBinaryCondition) encryptCondition).getExpressionSegment();
            return new EncryptPredicateFunctionRightValueToken(startIndex, stopIndex, functionSegment.getFunctionName(), functionSegment.getParameters(), indexValues, parameterMarkerIndexes);
        }
        if (encryptCondition instanceof EncryptBetweenCondition && (!indexValues.isEmpty() || !parameterMarkerIndexes.isEmpty())) {
            return new EncryptBetweenValueToken(startIndex, stopIndex, indexValues, parameterMarkerIndexes);
        }
        return encryptCondition instanceof EncryptInCondition
                ? new EncryptPredicateInRightValueToken(startIndex, stopIndex, indexValues, parameterMarkerIndexes)
                : new EncryptPredicateEqualRightValueToken(startIndex, stopIndex, indexValues, parameterMarkerIndexes);
    }
    
    private Map<Integer, Object> getPositionValues(final Collection<Integer> valuePositions, final List<Object> encryptValues) {
        Map<Integer, Object> result = new LinkedHashMap<>(valuePositions.size(), 1F);
        for (int each : valuePositions) {
            result.put(each, encryptValues.get(each));
        }
        return result;
    }
}
