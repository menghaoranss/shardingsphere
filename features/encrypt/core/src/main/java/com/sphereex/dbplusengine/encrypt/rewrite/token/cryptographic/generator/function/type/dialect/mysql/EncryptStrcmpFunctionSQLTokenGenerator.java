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

package com.sphereex.dbplusengine.encrypt.rewrite.token.cryptographic.generator.function.type.dialect.mysql;

import com.sphereex.dbplusengine.encrypt.checker.cryptographic.StrcmpFunctionEncryptorChecker;
import com.sphereex.dbplusengine.encrypt.rewrite.token.cryptographic.generator.function.EncryptFunctionSQLTokenGenerator;
import com.sphereex.dbplusengine.encrypt.rewrite.token.cryptographic.pojo.EncryptColumnSubstitutableToken;
import com.sphereex.dbplusengine.encrypt.rewrite.token.cryptographic.pojo.EncryptSimpleSubstitutableToken;
import com.sphereex.dbplusengine.encrypt.rule.column.item.OrderQueryColumnItem;
import com.sphereex.dbplusengine.encrypt.rule.column.item.PlainColumnItem;
import org.apache.shardingsphere.encrypt.exception.metadata.MissingMatchedEncryptQueryAlgorithmException;
import org.apache.shardingsphere.encrypt.rule.EncryptRule;
import org.apache.shardingsphere.encrypt.rule.column.EncryptColumn;
import org.apache.shardingsphere.encrypt.rule.table.EncryptTable;
import org.apache.shardingsphere.infra.annotation.HighFrequencyInvocation;
import org.apache.shardingsphere.infra.exception.core.ShardingSpherePreconditions;
import org.apache.shardingsphere.infra.metadata.ShardingSphereMetaData;
import org.apache.shardingsphere.infra.metadata.database.ShardingSphereDatabase;
import org.apache.shardingsphere.infra.rewrite.sql.token.common.pojo.SQLToken;
import org.apache.shardingsphere.sql.parser.statement.core.segment.dml.column.ColumnSegment;
import org.apache.shardingsphere.sql.parser.statement.core.segment.dml.expr.ExpressionSegment;
import org.apache.shardingsphere.sql.parser.statement.core.segment.dml.expr.FunctionSegment;
import org.apache.shardingsphere.sql.parser.statement.core.segment.dml.expr.simple.LiteralExpressionSegment;
import org.apache.shardingsphere.sql.parser.statement.core.segment.generic.OwnerSegment;
import org.apache.shardingsphere.sql.parser.statement.core.value.identifier.IdentifierValue;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Optional;

/**
 * Encrypt strcmp function encrypt SQL token generator.
 */
@HighFrequencyInvocation
public final class EncryptStrcmpFunctionSQLTokenGenerator implements EncryptFunctionSQLTokenGenerator {
    
    @Override
    public boolean isGenerateSQLToken(final ExpressionSegment expressionSegment) {
        return expressionSegment instanceof FunctionSegment && getType().equalsIgnoreCase(((FunctionSegment) expressionSegment).getFunctionName());
    }
    
    @Override
    public Collection<SQLToken> generateSQLTokens(final EncryptRule encryptRule, final Map<String, EncryptRule> databaseEncryptRules,
                                                  final ShardingSphereDatabase database, final ShardingSphereMetaData metaData, final ExpressionSegment expressionSegment) {
        FunctionSegment functionSegment = (FunctionSegment) expressionSegment;
        Iterator<ExpressionSegment> paramIterator = functionSegment.getParameters().iterator();
        ExpressionSegment param1 = paramIterator.next();
        ExpressionSegment param2 = paramIterator.next();
        if (!(param1 instanceof ColumnSegment) && !(param2 instanceof ColumnSegment)) {
            return Collections.emptyList();
        }
        if (param1 instanceof ColumnSegment && param2 instanceof ColumnSegment) {
            StrcmpFunctionEncryptorChecker.checkIsSame((ColumnSegment) param1, (ColumnSegment) param2, encryptRule, databaseEncryptRules);
        }
        ColumnSegment columnSegment = param1 instanceof ColumnSegment ? (ColumnSegment) param1 : (ColumnSegment) param2;
        Collection<SQLToken> result = new LinkedList<>(generateSQLTokens(encryptRule, databaseEncryptRules, columnSegment));
        if (result.isEmpty()) {
            return Collections.emptyList();
        }
        ExpressionSegment otherParamSegment = columnSegment.equals(param1) ? param2 : param1;
        if (otherParamSegment instanceof LiteralExpressionSegment) {
            result.addAll(generateSQLTokens(encryptRule, databaseEncryptRules, database, (LiteralExpressionSegment) otherParamSegment, columnSegment));
        } else if (otherParamSegment instanceof ColumnSegment) {
            result.addAll(generateSQLTokens(encryptRule, databaseEncryptRules, (ColumnSegment) otherParamSegment));
        }
        return result;
    }
    
    private Collection<SQLToken> generateSQLTokens(final EncryptRule encryptRule, final Map<String, EncryptRule> databaseEncryptRules,
                                                   final ShardingSphereDatabase database, final LiteralExpressionSegment literalExpressionSegment, final ColumnSegment columnSegment) {
        String columnName = columnSegment.getColumnBoundInfo().getOriginalColumn().getValue();
        String tableName = columnSegment.getColumnBoundInfo().getOriginalTable().getValue();
        Optional<EncryptTable> encryptTable = databaseEncryptRules.getOrDefault(columnSegment.getColumnBoundInfo().getOriginalDatabase().getValue(), encryptRule).findEncryptTable(tableName);
        if (!encryptTable.isPresent() || !encryptTable.get().isEncryptColumn(columnName)) {
            return Collections.emptyList();
        }
        EncryptColumn encryptColumn = encryptTable.get().getEncryptColumn(columnName);
        Optional<PlainColumnItem> plainColumnItem = encryptColumn.getPlain();
        if (plainColumnItem.isPresent() && plainColumnItem.get().isQueryWithPlain()) {
            return Collections.emptyList();
        }
        String schemaName = columnSegment.getColumnBoundInfo().getOriginalSchema().getValue();
        return Collections.singleton(new EncryptSimpleSubstitutableToken(literalExpressionSegment.getStartIndex(), literalExpressionSegment.getStopIndex(),
                encryptColumn.getCipher().encrypt(database.getName(), schemaName, encryptTable.get().getTable(), encryptColumn.getName(), literalExpressionSegment.getLiterals()).toString()));
    }
    
    private Collection<SQLToken> generateSQLTokens(final EncryptRule encryptRule, final Map<String, EncryptRule> databaseEncryptRules, final ColumnSegment columnSegment) {
        String columnName = columnSegment.getColumnBoundInfo().getOriginalColumn().getValue();
        String tableName = columnSegment.getColumnBoundInfo().getOriginalTable().getValue();
        Optional<EncryptTable> encryptTable = databaseEncryptRules.getOrDefault(columnSegment.getColumnBoundInfo().getOriginalDatabase().getValue(), encryptRule).findEncryptTable(tableName);
        if (!encryptTable.isPresent() || !encryptTable.get().isEncryptColumn(columnName)) {
            return Collections.emptyList();
        }
        EncryptColumn encryptColumn = encryptTable.get().getEncryptColumn(columnName);
        Optional<PlainColumnItem> plainColumnItem = encryptColumn.getPlain();
        if (plainColumnItem.isPresent() && plainColumnItem.get().isQueryWithPlain()) {
            return Collections.singleton(new EncryptColumnSubstitutableToken(columnSegment.getStartIndex(), columnSegment.getStopIndex(),
                    new IdentifierValue(plainColumnItem.get().getName(), columnSegment.getIdentifier().getQuoteCharacter()), columnSegment.getOwner().map(OwnerSegment::getIdentifier).orElse(null)));
        }
        ShardingSpherePreconditions.checkState(encryptColumn.getOrderQuery().isPresent() || encryptColumn.getCipher().getEncryptor().getMetaData().isSupportOrder(),
                () -> new MissingMatchedEncryptQueryAlgorithmException(tableName, columnSegment.getIdentifier().getValue(), "ORDER"));
        String orderQueryName = encryptColumn.getOrderQuery().map(OrderQueryColumnItem::getName).orElseGet(() -> encryptColumn.getCipher().getName());
        return Collections.singleton(new EncryptColumnSubstitutableToken(columnSegment.getStartIndex(), columnSegment.getStopIndex(),
                new IdentifierValue(orderQueryName, columnSegment.getIdentifier().getQuoteCharacter()), columnSegment.getOwner().map(OwnerSegment::getIdentifier).orElse(null)));
    }
    
    @Override
    public String getType() {
        return "STRCMP";
    }
}
