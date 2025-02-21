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

package org.apache.shardingsphere.encrypt.rewrite.parameter.rewriter;

import com.sphereex.dbplusengine.SphereEx;
import com.sphereex.dbplusengine.encrypt.rewrite.condition.impl.EncryptBetweenCondition;
import com.sphereex.dbplusengine.infra.hint.NoneUniqueKeyScenario;
import lombok.RequiredArgsConstructor;
import org.apache.shardingsphere.encrypt.exception.metadata.MissingMatchedEncryptQueryAlgorithmException;
import org.apache.shardingsphere.encrypt.rewrite.condition.EncryptCondition;
import org.apache.shardingsphere.encrypt.rewrite.condition.EncryptConditionValues;
import org.apache.shardingsphere.encrypt.rewrite.condition.impl.EncryptBinaryCondition;
import org.apache.shardingsphere.encrypt.rule.EncryptRule;
import org.apache.shardingsphere.encrypt.rule.column.EncryptColumn;
import org.apache.shardingsphere.encrypt.rule.table.EncryptTable;
import org.apache.shardingsphere.infra.binder.context.statement.SQLStatementContext;
import org.apache.shardingsphere.infra.binder.context.type.TableAvailable;
import org.apache.shardingsphere.infra.binder.context.type.WhereAvailable;
import org.apache.shardingsphere.infra.database.core.type.DatabaseTypeRegistry;
import org.apache.shardingsphere.infra.exception.core.ShardingSpherePreconditions;
import org.apache.shardingsphere.infra.hint.HintValueContext;
import org.apache.shardingsphere.infra.rewrite.parameter.builder.ParameterBuilder;
import org.apache.shardingsphere.infra.rewrite.parameter.builder.impl.StandardParameterBuilder;
import org.apache.shardingsphere.infra.rewrite.parameter.rewriter.ParameterRewriter;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Predicate parameter rewriter for encrypt.
 */
@RequiredArgsConstructor
public final class EncryptPredicateParameterRewriter implements ParameterRewriter {
    
    @SphereEx
    private static final Collection<String> GREATER_LESS_COMPARISON_OPERATORS = new HashSet<>(Arrays.asList(">", "<", ">=", "<="));
    
    private final EncryptRule rule;
    
    private final String databaseName;
    
    private final Collection<EncryptCondition> encryptConditions;
    
    private final HintValueContext hintValueContext;
    
    @Override
    public boolean isNeedRewrite(final SQLStatementContext sqlStatementContext) {
        return sqlStatementContext instanceof WhereAvailable;
    }
    
    @Override
    public void rewrite(final ParameterBuilder paramBuilder, final SQLStatementContext sqlStatementContext, final List<Object> params) {
        String schemaName = ((TableAvailable) sqlStatementContext).getTablesContext().getSchemaName()
                .orElseGet(() -> new DatabaseTypeRegistry(sqlStatementContext.getDatabaseType()).getDefaultSchemaName(databaseName));
        for (EncryptCondition each : encryptConditions) {
            // SPEX ADDED: BEGIN
            if (rule.isQueryWithPlain(each.getTableName(), each.getColumnSegment().getIdentifier().getValue())) {
                continue;
            }
            // SPEX ADDED: END
            encryptParameters(paramBuilder, each.getPositionIndexMap(), getEncryptedValues(schemaName, each, new EncryptConditionValues(each).get(params)));
        }
    }
    
    private List<Object> getEncryptedValues(final String schemaName, final EncryptCondition encryptCondition, final List<Object> originalValues) {
        String tableName = encryptCondition.getColumnSegment().getColumnBoundInfo().getOriginalTable().getValue();
        String columnName = encryptCondition.getColumnSegment().getColumnBoundInfo().getOriginalColumn().getValue();
        EncryptTable encryptTable = rule.getEncryptTable(tableName);
        EncryptColumn encryptColumn = encryptTable.getEncryptColumn(columnName);
        if (encryptCondition instanceof EncryptBinaryCondition && "LIKE".equals(((EncryptBinaryCondition) encryptCondition).getOperator()) && encryptColumn.getLikeQuery().isPresent()) {
            return encryptColumn.getLikeQuery().get().encrypt(databaseName, schemaName, tableName, columnName, originalValues);
        }
        // SPEX ADDED: BEGIN
        if (encryptCondition instanceof EncryptBinaryCondition && GREATER_LESS_COMPARISON_OPERATORS.contains(((EncryptBinaryCondition) encryptCondition).getOperator())
                || encryptCondition instanceof EncryptBetweenCondition) {
            ShardingSpherePreconditions.checkState(encryptColumn.getOrderQuery().isPresent() || encryptColumn.getCipher().getEncryptor().getMetaData().isSupportOrder(),
                    () -> new MissingMatchedEncryptQueryAlgorithmException(tableName, columnName, "ORDER"));
            return encryptColumn.getOrderQuery()
                    .map(orderQueryColumnItem -> orderQueryColumnItem.encrypt(databaseName, schemaName, tableName, columnName, originalValues))
                    .orElseGet(() -> encryptColumn.getCipher().encrypt(databaseName, schemaName, tableName, columnName, originalValues));
        }
        // SPEX ADDED: END
        // SPEX CHANGED: BEGIN
        return encryptColumn.getAssistedQuery().isPresent() && NoneUniqueKeyScenario.NONE == hintValueContext.getNoneUniqueKeyScenario()
                // SPEX CHANGED: END
                ? encryptColumn.getAssistedQuery().get().encrypt(databaseName, schemaName, tableName, columnName, originalValues)
                : encryptColumn.getCipher().encrypt(databaseName, schemaName, tableName, columnName, originalValues);
    }
    
    private void encryptParameters(final ParameterBuilder paramBuilder, final Map<Integer, Integer> positionIndexes, final List<Object> encryptValues) {
        if (!positionIndexes.isEmpty()) {
            for (Entry<Integer, Integer> entry : positionIndexes.entrySet()) {
                ((StandardParameterBuilder) paramBuilder).addReplacedParameters(entry.getValue(), encryptValues.get(entry.getKey()));
            }
        }
    }
}
