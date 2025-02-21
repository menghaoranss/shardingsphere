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

package com.sphereex.dbplusengine.encrypt.rewrite.token.cryptographic.generator.function.type.standard;

import com.sphereex.dbplusengine.encrypt.rewrite.token.cryptographic.generator.function.EncryptFunctionSQLTokenGenerator;
import com.sphereex.dbplusengine.encrypt.rewrite.token.cryptographic.pojo.EncryptColumnSubstitutableToken;
import com.sphereex.dbplusengine.encrypt.rule.column.item.PlainColumnItem;
import org.apache.shardingsphere.encrypt.rule.EncryptRule;
import org.apache.shardingsphere.encrypt.rule.column.EncryptColumn;
import org.apache.shardingsphere.encrypt.rule.table.EncryptTable;
import org.apache.shardingsphere.infra.annotation.HighFrequencyInvocation;
import org.apache.shardingsphere.infra.binder.context.segment.select.projection.Projection;
import org.apache.shardingsphere.infra.binder.context.segment.select.projection.impl.ColumnProjection;
import org.apache.shardingsphere.infra.database.core.metadata.database.enums.QuoteCharacter;
import org.apache.shardingsphere.infra.database.core.type.DatabaseType;
import org.apache.shardingsphere.infra.metadata.ShardingSphereMetaData;
import org.apache.shardingsphere.infra.metadata.database.ShardingSphereDatabase;
import org.apache.shardingsphere.infra.rewrite.sql.token.common.pojo.SQLToken;
import org.apache.shardingsphere.infra.rewrite.sql.token.common.pojo.generic.SubstitutableColumnNameToken;
import org.apache.shardingsphere.sql.parser.statement.core.segment.dml.column.ColumnSegment;
import org.apache.shardingsphere.sql.parser.statement.core.segment.dml.expr.ExpressionSegment;
import org.apache.shardingsphere.sql.parser.statement.core.segment.dml.item.AggregationProjectionSegment;
import org.apache.shardingsphere.sql.parser.statement.core.segment.generic.OwnerSegment;
import org.apache.shardingsphere.sql.parser.statement.core.value.identifier.IdentifierValue;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;

/**
 * Encrypt count function encrypt SQL token generator.
 */
@HighFrequencyInvocation
public final class EncryptCountFunctionSQLTokenGenerator implements EncryptFunctionSQLTokenGenerator {
    
    @Override
    public boolean isGenerateSQLToken(final ExpressionSegment expressionSegment) {
        return expressionSegment instanceof AggregationProjectionSegment && getType().equalsIgnoreCase(((AggregationProjectionSegment) expressionSegment).getType().name());
    }
    
    @Override
    public Collection<SQLToken> generateSQLTokens(final EncryptRule encryptRule, final Map<String, EncryptRule> databaseEncryptRules,
                                                  final ShardingSphereDatabase database, final ShardingSphereMetaData metaData, final ExpressionSegment expressionSegment) {
        AggregationProjectionSegment aggregationSegment = (AggregationProjectionSegment) expressionSegment;
        if (1 != aggregationSegment.getParameters().size() || !(aggregationSegment.getParameters().iterator().next() instanceof ColumnSegment)) {
            return Collections.emptyList();
        }
        ColumnSegment columnSegment = (ColumnSegment) aggregationSegment.getParameters().iterator().next();
        String tableName = columnSegment.getColumnBoundInfo().getOriginalTable().getValue();
        Optional<EncryptTable> encryptTable = encryptRule.findEncryptTable(tableName);
        if (!encryptTable.isPresent() || !encryptTable.get().isEncryptColumn(columnSegment.getColumnBoundInfo().getOriginalColumn().getValue())) {
            return Collections.emptyList();
        }
        EncryptColumn encryptColumn = encryptTable.get().getEncryptColumn(columnSegment.getColumnBoundInfo().getOriginalColumn().getValue());
        Optional<PlainColumnItem> plainColumnItem = encryptColumn.getPlain();
        if (plainColumnItem.isPresent() && plainColumnItem.get().isQueryWithPlain()) {
            return Collections.singleton(new EncryptColumnSubstitutableToken(columnSegment.getStartIndex(), columnSegment.getStopIndex(),
                    new IdentifierValue(plainColumnItem.get().getName(), columnSegment.getIdentifier().getQuoteCharacter()), columnSegment.getOwner().map(OwnerSegment::getIdentifier).orElse(null)));
        }
        DatabaseType databaseType = database.getProtocolType();
        return Collections.singleton(new SubstitutableColumnNameToken(columnSegment.getStartIndex(), columnSegment.getStopIndex(),
                createColumnProjections(encryptColumn.getCipher().getName(), columnSegment.getIdentifier().getQuoteCharacter(), databaseType), databaseType, database, metaData));
    }
    
    private Collection<Projection> createColumnProjections(final String columnName, final QuoteCharacter quoteCharacter, final DatabaseType databaseType) {
        return Collections.singleton(new ColumnProjection(null, new IdentifierValue(columnName, quoteCharacter), null, databaseType));
    }
    
    @Override
    public String getType() {
        return "COUNT";
    }
}
