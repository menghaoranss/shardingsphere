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

package org.apache.shardingsphere.encrypt.rewrite.token.generator.assignment;

import com.sphereex.dbplusengine.SphereEx;
import com.sphereex.dbplusengine.SphereEx.Type;
import com.sphereex.dbplusengine.encrypt.rewrite.token.cryptographic.pojo.EncryptColumnAssignmentToken;
import lombok.RequiredArgsConstructor;
import org.apache.shardingsphere.encrypt.rewrite.token.comparator.EncryptorComparator;
import org.apache.shardingsphere.encrypt.rewrite.token.pojo.EncryptAssignmentToken;
import org.apache.shardingsphere.encrypt.rewrite.token.pojo.EncryptFunctionAssignmentToken;
import org.apache.shardingsphere.encrypt.rewrite.token.pojo.EncryptLiteralAssignmentToken;
import org.apache.shardingsphere.encrypt.rewrite.token.pojo.EncryptParameterAssignmentToken;
import org.apache.shardingsphere.encrypt.rule.EncryptRule;
import org.apache.shardingsphere.encrypt.rule.column.EncryptColumn;
import org.apache.shardingsphere.encrypt.rule.table.EncryptTable;
import org.apache.shardingsphere.infra.annotation.HighFrequencyInvocation;
import org.apache.shardingsphere.infra.binder.context.segment.table.TablesContext;
import org.apache.shardingsphere.infra.database.core.metadata.database.enums.QuoteCharacter;
import org.apache.shardingsphere.infra.database.core.type.DatabaseType;
import org.apache.shardingsphere.infra.database.core.type.DatabaseTypeRegistry;
import org.apache.shardingsphere.infra.exception.core.ShardingSpherePreconditions;
import org.apache.shardingsphere.infra.exception.generic.UnsupportedSQLOperationException;
import org.apache.shardingsphere.infra.rewrite.sql.token.common.pojo.SQLToken;
import org.apache.shardingsphere.sql.parser.statement.core.segment.dml.assignment.ColumnAssignmentSegment;
import org.apache.shardingsphere.sql.parser.statement.core.segment.dml.assignment.SetAssignmentSegment;
import org.apache.shardingsphere.sql.parser.statement.core.segment.dml.column.ColumnSegment;
import org.apache.shardingsphere.sql.parser.statement.core.segment.dml.expr.FunctionSegment;
import org.apache.shardingsphere.sql.parser.statement.core.segment.dml.expr.simple.LiteralExpressionSegment;
import org.apache.shardingsphere.sql.parser.statement.core.segment.dml.expr.simple.ParameterMarkerExpressionSegment;
import org.apache.shardingsphere.sql.parser.statement.core.segment.generic.OwnerSegment;
import org.apache.shardingsphere.sql.parser.statement.core.value.identifier.IdentifierValue;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.Map;
import java.util.Optional;

/**
 * Assignment generator for encrypt.
 */
@HighFrequencyInvocation
@RequiredArgsConstructor
public final class EncryptAssignmentTokenGenerator {
    
    private final EncryptRule rule;
    
    private final String databaseName;
    
    private final DatabaseType databaseType;
    
    @SphereEx
    private final Map<String, EncryptRule> databaseEncryptRules;
    
    /**
     * Generate SQL tokens.
     *
     * @param tablesContext SQL statement context
     * @param setAssignmentSegment set assignment segment
     * @return generated SQL tokens
     */
    public Collection<SQLToken> generateSQLTokens(final TablesContext tablesContext, final SetAssignmentSegment setAssignmentSegment) {
        String tableName = tablesContext.getSimpleTables().iterator().next().getTableName().getIdentifier().getValue();
        EncryptTable encryptTable = rule.getEncryptTable(tableName);
        Collection<SQLToken> result = new LinkedList<>();
        String schemaName = tablesContext.getSchemaName().orElseGet(() -> new DatabaseTypeRegistry(databaseType).getDefaultSchemaName(databaseName));
        for (ColumnAssignmentSegment each : setAssignmentSegment.getAssignments()) {
            String columnName = each.getColumns().get(0).getIdentifier().getValue();
            if (encryptTable.isEncryptColumn(columnName)) {
                generateSQLToken(schemaName, encryptTable.getTable(), encryptTable.getEncryptColumn(columnName), each).ifPresent(result::add);
            }
        }
        return result;
    }
    
    private Optional<EncryptAssignmentToken> generateSQLToken(final String schemaName, final String tableName, final EncryptColumn encryptColumn, final ColumnAssignmentSegment segment) {
        if (segment.getValue() instanceof ParameterMarkerExpressionSegment) {
            return Optional.of(generateParameterSQLToken(encryptColumn, segment));
        }
        if (segment.getValue() instanceof LiteralExpressionSegment) {
            return Optional.of(generateLiteralSQLToken(schemaName, tableName, encryptColumn, segment));
        }
        // SPEX ADDED: BEGIN
        if (segment.getValue() instanceof FunctionSegment) {
            return Optional.of(generateFunctionSQLToken(encryptColumn, segment));
        }
        if (segment.getValue() instanceof ColumnSegment) {
            return Optional.of(generateColumnSQLToken(encryptColumn, segment, (ColumnSegment) segment.getValue()));
        }
        // SPEX ADDED: END
        return Optional.empty();
    }
    
    private EncryptAssignmentToken generateParameterSQLToken(final EncryptColumn encryptColumn, final ColumnAssignmentSegment segment) {
        @SphereEx(Type.MODIFY)
        EncryptParameterAssignmentToken result =
                new EncryptParameterAssignmentToken(segment.getColumns().get(0).getStartIndex(), segment.getStopIndex(), segment.getColumns().get(0).getIdentifier().getQuoteCharacter(), getOwner(segment));
        result.addColumnName(encryptColumn.getCipher().getName());
        encryptColumn.getAssistedQuery().ifPresent(optional -> result.addColumnName(optional.getName()));
        encryptColumn.getLikeQuery().ifPresent(optional -> result.addColumnName(optional.getName()));
        // SPEX ADDED: BEGIN
        encryptColumn.getOrderQuery().ifPresent(optional -> result.addColumnName(optional.getName()));
        encryptColumn.getPlain().ifPresent(optional -> result.addColumnName(optional.getName()));
        // SPEX ADDED: END
        return result;
    }
    
    @SphereEx
    private IdentifierValue getOwner(final ColumnAssignmentSegment segment) {
        return segment.getColumns().get(0).getOwner().map(OwnerSegment::getIdentifier).orElse(null);
    }
    
    private EncryptAssignmentToken generateLiteralSQLToken(final String schemaName, final String tableName, final EncryptColumn encryptColumn, final ColumnAssignmentSegment segment) {
        @SphereEx(Type.MODIFY)
        EncryptLiteralAssignmentToken result = new EncryptLiteralAssignmentToken(segment.getColumns().get(0).getStartIndex(), segment.getStopIndex(), segment.getColumns().get(0).getIdentifier().getQuoteCharacter(), getOwner(segment));
        addCipherAssignment(schemaName, tableName, encryptColumn, segment, result);
        addAssistedQueryAssignment(schemaName, tableName, encryptColumn, segment, result);
        addLikeAssignment(schemaName, tableName, encryptColumn, segment, result);
        // SPEX ADDED: BEGIN
        addOrderAssignment(schemaName, tableName, encryptColumn, segment, result);
        addPlainAssignment(encryptColumn, segment, result);
        // SPEX ADDED: END
        return result;
    }
    
    @SphereEx
    private EncryptAssignmentToken generateFunctionSQLToken(final EncryptColumn encryptColumn, final ColumnAssignmentSegment segment) {
        QuoteCharacter quoteCharacter = segment.getColumns().get(0).getIdentifier().getQuoteCharacter();
        EncryptFunctionAssignmentToken result =
                new EncryptFunctionAssignmentToken(segment.getColumns().get(0).getStartIndex(), segment.getStopIndex(), quoteCharacter);
        String functionName = ((FunctionSegment) segment.getValue()).getFunctionName();
        result.addAssignment(encryptColumn.getCipher().getName(), getFunctionToken(functionName, encryptColumn.getCipher().getName(), quoteCharacter));
        encryptColumn.getAssistedQuery().ifPresent(optional -> result.addAssignment(optional.getName(), getFunctionToken(functionName, optional.getName(), quoteCharacter)));
        encryptColumn.getLikeQuery().ifPresent(optional -> result.addAssignment(optional.getName(), getFunctionToken(functionName, optional.getName(), quoteCharacter)));
        encryptColumn.getOrderQuery().ifPresent(optional -> result.addAssignment(optional.getName(), getFunctionToken(functionName, optional.getName(), quoteCharacter)));
        encryptColumn.getPlain().ifPresent(optional -> result.addAssignment(optional.getName(), getFunctionToken(functionName, optional.getName(), quoteCharacter)));
        return result;
    }
    
    @SphereEx
    private String getFunctionToken(final String functionName, final String name, final QuoteCharacter quoteCharacter) {
        return functionName + "(" + quoteCharacter.wrap(name) + "," + " ?)";
    }
    
    private void addCipherAssignment(final String schemaName, final String tableName,
                                     final EncryptColumn encryptColumn, final ColumnAssignmentSegment segment, final EncryptLiteralAssignmentToken token) {
        Object originalValue = ((LiteralExpressionSegment) segment.getValue()).getLiterals();
        Object cipherValue = encryptColumn.getCipher().encrypt(databaseName, schemaName, tableName, encryptColumn.getName(), Collections.singletonList(originalValue)).iterator().next();
        token.addAssignment(encryptColumn.getCipher().getName(), cipherValue);
    }
    
    private void addAssistedQueryAssignment(final String schemaName, final String tableName, final EncryptColumn encryptColumn,
                                            final ColumnAssignmentSegment segment, final EncryptLiteralAssignmentToken token) {
        Object originalValue = ((LiteralExpressionSegment) segment.getValue()).getLiterals();
        if (encryptColumn.getAssistedQuery().isPresent()) {
            Object assistedQueryValue = encryptColumn.getAssistedQuery().get().encrypt(
                    databaseName, schemaName, tableName, encryptColumn.getName(), Collections.singletonList(originalValue)).iterator().next();
            token.addAssignment(encryptColumn.getAssistedQuery().get().getName(), assistedQueryValue);
        }
    }
    
    private void addLikeAssignment(final String schemaName, final String tableName,
                                   final EncryptColumn encryptColumn, final ColumnAssignmentSegment segment, final EncryptLiteralAssignmentToken token) {
        Object originalValue = ((LiteralExpressionSegment) segment.getValue()).getLiterals();
        if (encryptColumn.getLikeQuery().isPresent()) {
            Object assistedQueryValue = encryptColumn.getLikeQuery().get().encrypt(databaseName, schemaName,
                    tableName, segment.getColumns().get(0).getIdentifier().getValue(), Collections.singletonList(originalValue)).iterator().next();
            token.addAssignment(encryptColumn.getLikeQuery().get().getName(), assistedQueryValue);
        }
    }
    
    @SphereEx
    private void addOrderAssignment(final String schemaName, final String tableName,
                                    final EncryptColumn encryptColumn, final ColumnAssignmentSegment segment, final EncryptLiteralAssignmentToken token) {
        Object originalValue = ((LiteralExpressionSegment) segment.getValue()).getLiterals();
        if (encryptColumn.getOrderQuery().isPresent()) {
            Object assistedQueryValue = encryptColumn.getOrderQuery().get().encrypt(databaseName, schemaName,
                    tableName, segment.getColumns().get(0).getIdentifier().getValue(), Collections.singletonList(originalValue)).iterator().next();
            token.addAssignment(encryptColumn.getOrderQuery().get().getName(), assistedQueryValue);
        }
    }
    
    @SphereEx
    private void addPlainAssignment(final EncryptColumn encryptColumn, final ColumnAssignmentSegment segment, final EncryptLiteralAssignmentToken token) {
        Object originalValue = ((LiteralExpressionSegment) segment.getValue()).getLiterals();
        encryptColumn.getPlain().ifPresent(optional -> token.addAssignment(optional.getName(), originalValue));
    }
    
    @SphereEx
    private EncryptAssignmentToken generateColumnSQLToken(final EncryptColumn leftEncryptColumn, final ColumnAssignmentSegment assignmentSegment, final ColumnSegment rightColumn) {
        ColumnSegment leftColumn = assignmentSegment.getColumns().get(0);
        Optional<EncryptTable> rightEncryptTable = findEncryptTable(rightColumn);
        if (rightEncryptTable.isPresent() && rightEncryptTable.get().isEncryptColumn(rightColumn.getIdentifier().getValue())) {
            ShardingSpherePreconditions.checkState(EncryptorComparator.isSame(rule, leftColumn.getColumnBoundInfo(), rightColumn.getColumnBoundInfo(), databaseEncryptRules),
                    () -> new UnsupportedSQLOperationException(
                            "Can not use different encryptor for " + leftColumn.getColumnBoundInfo() + " and " + rightColumn.getColumnBoundInfo() + " in set clause"));
            return generateColumnSQLToken(assignmentSegment, leftColumn, rightColumn, leftEncryptColumn, rightEncryptTable.get().getEncryptColumn(rightColumn.getIdentifier().getValue()));
        }
        throw new UnsupportedSQLOperationException(
                "Can not use different encryptor for " + leftColumn.getColumnBoundInfo() + " and " + rightColumn.getColumnBoundInfo() + " in set clause");
    }

    @SphereEx
    private EncryptAssignmentToken generateColumnSQLToken(final ColumnAssignmentSegment segment, final ColumnSegment leftColumn, final ColumnSegment rightColumn,
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

    @SphereEx
    private Optional<EncryptTable> findEncryptTable(final ColumnSegment columnSegment) {
        EncryptRule encryptRule = Optional.ofNullable(databaseEncryptRules.get(columnSegment.getColumnBoundInfo().getOriginalDatabase().getValue())).orElse(rule);
        return encryptRule.findEncryptTable(columnSegment.getColumnBoundInfo().getOriginalTable().getValue());
    }
}
