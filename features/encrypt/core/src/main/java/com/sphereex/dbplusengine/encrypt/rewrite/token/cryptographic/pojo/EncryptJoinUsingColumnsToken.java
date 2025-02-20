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

package com.sphereex.dbplusengine.encrypt.rewrite.token.cryptographic.pojo;

import lombok.Getter;
import org.apache.shardingsphere.encrypt.rule.EncryptRule;
import org.apache.shardingsphere.encrypt.rule.column.EncryptColumn;
import org.apache.shardingsphere.encrypt.rule.column.item.AssistedQueryColumnItem;
import org.apache.shardingsphere.encrypt.rule.table.EncryptTable;
import org.apache.shardingsphere.infra.binder.context.segment.table.TablesContext;
import org.apache.shardingsphere.infra.database.core.metadata.database.enums.QuoteCharacter;
import org.apache.shardingsphere.infra.rewrite.sql.token.common.pojo.SQLToken;
import org.apache.shardingsphere.infra.rewrite.sql.token.common.pojo.Substitutable;
import org.apache.shardingsphere.sql.parser.statement.core.segment.dml.column.ColumnSegment;
import org.apache.shardingsphere.sql.parser.statement.core.segment.generic.bound.ColumnSegmentBoundInfo;
import org.apache.shardingsphere.sql.parser.statement.core.value.identifier.IdentifierValue;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Join using columns token for encrypt.
 */
@Getter
public final class EncryptJoinUsingColumnsToken extends SQLToken implements Substitutable {
    
    private final int stopIndex;
    
    private final List<ColumnSegment> usingColumns;
    
    private final Map<String, IdentifierValue> tableNameAliasMap;
    
    private final EncryptRule rule;
    
    private final Map<String, EncryptRule> databaseEncryptRules;
    
    public EncryptJoinUsingColumnsToken(final int startIndex, final int stopIndex, final List<ColumnSegment> usingColumns, final TablesContext tablesContext,
                                        final EncryptRule rule, final Map<String, EncryptRule> databaseEncryptRules) {
        super(startIndex);
        this.stopIndex = stopIndex;
        this.usingColumns = usingColumns;
        tableNameAliasMap = tablesContext.getTableNameAliasMap();
        this.rule = rule;
        this.databaseEncryptRules = databaseEncryptRules;
    }
    
    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder(" ON ");
        int index = 0;
        for (ColumnSegment each : usingColumns) {
            if (index++ > 0) {
                builder.append(" AND ");
            }
            String leftOwner = tableNameAliasMap.get(each.getColumnBoundInfo().getOriginalTable().getValue().toLowerCase()).getValueWithQuoteCharacters();
            String leftColumn = getEncryptColumn(each.getColumnBoundInfo(), rule, databaseEncryptRules);
            String rightOwner = tableNameAliasMap.get(each.getOtherUsingColumnBoundInfo().getOriginalTable().getValue().toLowerCase()).getValueWithQuoteCharacters();
            String rightColumn = getEncryptColumn(each.getOtherUsingColumnBoundInfo(), rule, databaseEncryptRules);
            builder.append(String.format("%s.%s = %s.%s", leftOwner, leftColumn, rightOwner, rightColumn));
        }
        return builder.toString();
    }
    
    private String getEncryptColumn(final ColumnSegmentBoundInfo columnBoundInfo, final EncryptRule encryptRule, final Map<String, EncryptRule> databaseEncryptRules) {
        String tableName = columnBoundInfo.getOriginalTable().getValue();
        String columnName = columnBoundInfo.getOriginalColumn().getValue();
        QuoteCharacter columnQuoteCharacter = columnBoundInfo.getOriginalColumn().getQuoteCharacter();
        Optional<EncryptTable> encryptTable = databaseEncryptRules.getOrDefault(columnBoundInfo.getOriginalDatabase().getValue(), encryptRule).findEncryptTable(tableName);
        if (!encryptTable.isPresent() || !encryptTable.get().isEncryptColumn(columnName)) {
            return columnQuoteCharacter.wrap(columnName);
        }
        EncryptColumn encryptColumn = encryptTable.get().getEncryptColumn(columnName);
        if (encryptColumn.getPlain().isPresent() && encryptColumn.getPlain().get().isQueryWithPlain()) {
            return columnQuoteCharacter.wrap(encryptColumn.getPlain().get().getName());
        }
        return columnQuoteCharacter.wrap(encryptColumn.getAssistedQuery().map(AssistedQueryColumnItem::getName).orElse(encryptColumn.getCipher().getName()));
    }
}
