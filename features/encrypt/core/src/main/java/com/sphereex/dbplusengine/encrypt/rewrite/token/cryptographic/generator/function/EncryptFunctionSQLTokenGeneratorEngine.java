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

package com.sphereex.dbplusengine.encrypt.rewrite.token.cryptographic.generator.function;

import com.sphereex.dbplusengine.encrypt.rewrite.token.cryptographic.pojo.EncryptColumnSubstitutableToken;
import com.sphereex.dbplusengine.encrypt.rule.column.item.PlainColumnItem;
import lombok.RequiredArgsConstructor;
import org.apache.shardingsphere.encrypt.exception.syntax.UnsupportedEncryptSQLException;
import org.apache.shardingsphere.encrypt.rule.EncryptRule;
import org.apache.shardingsphere.encrypt.rule.column.EncryptColumn;
import org.apache.shardingsphere.infra.database.core.type.DatabaseType;
import org.apache.shardingsphere.infra.metadata.ShardingSphereMetaData;
import org.apache.shardingsphere.infra.metadata.database.ShardingSphereDatabase;
import org.apache.shardingsphere.infra.rewrite.sql.token.common.pojo.SQLToken;
import org.apache.shardingsphere.infra.spi.type.typed.TypedSPILoader;
import org.apache.shardingsphere.sql.parser.statement.core.extractor.ColumnExtractor;
import org.apache.shardingsphere.sql.parser.statement.core.extractor.ExpressionExtractor;
import org.apache.shardingsphere.sql.parser.statement.core.segment.dml.column.ColumnSegment;
import org.apache.shardingsphere.sql.parser.statement.core.segment.dml.expr.ExpressionSegment;
import org.apache.shardingsphere.sql.parser.statement.core.segment.dml.expr.FunctionSegment;
import org.apache.shardingsphere.sql.parser.statement.core.segment.dml.item.AggregationProjectionSegment;
import org.apache.shardingsphere.sql.parser.statement.core.segment.dml.item.ProjectionSegment;
import org.apache.shardingsphere.sql.parser.statement.core.segment.generic.OwnerSegment;
import org.apache.shardingsphere.sql.parser.statement.core.value.identifier.IdentifierValue;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.Map;
import java.util.Optional;

/**
 * Encrypt function SQL token generator engine.
 */
@RequiredArgsConstructor
public final class EncryptFunctionSQLTokenGeneratorEngine {
    
    private final EncryptRule encryptRule;
    
    private final Map<String, EncryptRule> databaseEncryptRules;
    
    private final ShardingSphereDatabase database;
    
    private final ShardingSphereMetaData metaData;
    
    private final DatabaseType databaseType;
    
    /**
     * Generate SQL tokens.
     *
     * @param projectionSegment projection segment
     * @return SQL tokens
     */
    public Collection<SQLToken> generateSQLTokens(final ProjectionSegment projectionSegment) {
        Collection<SQLToken> result = new LinkedList<>();
        for (ExpressionSegment each : ExpressionExtractor.getFunctionSegments(projectionSegment)) {
            Optional<EncryptFunctionSQLTokenGenerator> functionSQLTokenGenerator = findFunctionEncryptSQLTokenGenerator(each, databaseType);
            if (functionSQLTokenGenerator.isPresent() && functionSQLTokenGenerator.get().isGenerateSQLToken(each)) {
                result.addAll(functionSQLTokenGenerator.get().generateSQLTokens(encryptRule, databaseEncryptRules, database, metaData, each));
            } else {
                result.addAll(generateUDFSQLTokens(each));
            }
        }
        return result;
    }
    
    private Collection<SQLToken> generateUDFSQLTokens(final ExpressionSegment expressionSegment) {
        Collection<SQLToken> result = new LinkedList<>();
        for (ColumnSegment each : ColumnExtractor.extract(expressionSegment)) {
            EncryptRule rule = databaseEncryptRules.getOrDefault(each.getColumnBoundInfo().getOriginalDatabase().getValue(), encryptRule);
            String originalTable = each.getColumnBoundInfo().getOriginalTable().getValue();
            String originalColumn = each.getColumnBoundInfo().getOriginalColumn().getValue();
            Optional<EncryptColumn> encryptColumn = rule.findEncryptTable(originalTable)
                    .filter(optional -> optional.isEncryptColumn(originalColumn)).map(optional -> optional.getEncryptColumn(originalColumn));
            if (!encryptColumn.isPresent()) {
                continue;
            }
            Optional<PlainColumnItem> plainColumnItem = encryptColumn.get().getPlain();
            if (plainColumnItem.isPresent() && plainColumnItem.get().isQueryWithPlain()) {
                return Collections.singleton(new EncryptColumnSubstitutableToken(each.getStartIndex(), each.getStopIndex(),
                        new IdentifierValue(plainColumnItem.get().getName(), each.getIdentifier().getQuoteCharacter()), each.getOwner().map(OwnerSegment::getIdentifier).orElse(null)));
            }
            throw new UnsupportedEncryptSQLException(expressionSegment.getText());
        }
        return result;
    }
    
    private Optional<EncryptFunctionSQLTokenGenerator> findFunctionEncryptSQLTokenGenerator(final ExpressionSegment expressionSegment, final DatabaseType databaseType) {
        if (expressionSegment instanceof FunctionSegment) {
            return findFunctionEncryptSQLTokenGenerator(((FunctionSegment) expressionSegment).getFunctionName(), databaseType);
        }
        if (expressionSegment instanceof AggregationProjectionSegment) {
            AggregationProjectionSegment aggregationSegment = (AggregationProjectionSegment) expressionSegment;
            return findFunctionEncryptSQLTokenGenerator(aggregationSegment.getType().name(), databaseType);
        }
        return Optional.empty();
    }
    
    private Optional<EncryptFunctionSQLTokenGenerator> findFunctionEncryptSQLTokenGenerator(final String functionName, final DatabaseType databaseType) {
        Optional<EncryptFunctionSQLTokenGenerator> result = Optional.ofNullable(TypedSPILoader.findService(EncryptFunctionSQLTokenGenerator.class, functionName)
                .orElseGet(() -> TypedSPILoader.findService(EncryptFunctionSQLTokenGenerator.class, databaseType.getType() + ":" + functionName).orElse(null)));
        if (!result.isPresent() && databaseType.getTrunkDatabaseType().isPresent()) {
            return TypedSPILoader.findService(EncryptFunctionSQLTokenGenerator.class, databaseType.getTrunkDatabaseType().get().getType() + ":" + functionName);
        }
        return result;
    }
}
