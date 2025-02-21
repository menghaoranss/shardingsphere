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

package com.sphereex.dbplusengine.test.e2e.engine.hook;

import com.sphereex.dbplusengine.infra.util.DatabaseTypeUtils;
import com.sphereex.dbplusengine.test.e2e.engine.context.ComparisonDataSourceContext;
import org.apache.shardingsphere.encrypt.config.rule.EncryptColumnItemRuleConfiguration;
import org.apache.shardingsphere.encrypt.config.rule.EncryptColumnRuleConfiguration;
import org.apache.shardingsphere.encrypt.config.rule.EncryptTableRuleConfiguration;
import org.apache.shardingsphere.infra.database.core.metadata.database.enums.QuoteCharacter;
import org.apache.shardingsphere.infra.database.core.type.DatabaseType;
import org.apache.shardingsphere.sql.parser.statement.core.segment.ddl.column.ColumnDefinitionSegment;
import org.apache.shardingsphere.sql.parser.statement.core.statement.SQLStatement;
import org.apache.shardingsphere.sql.parser.statement.core.statement.ddl.CreateTableStatement;
import org.apache.shardingsphere.sql.parser.statement.core.value.identifier.IdentifierValue;

import java.util.Collection;
import java.util.LinkedList;
import java.util.stream.Collectors;

/**
 * Encrypt rule generate sql handler.
 */
public final class EncryptRuleGenerateSqlExecuteHook extends AbstratParserSqlExecuteHook {
    
    @Override
    public String before(final String sql, final ComparisonDataSourceContext context) {
        SQLStatement sqlStatement = getSQLParserEngine(context.getDatabaseType()).parse(sql, true);
        if (!(sqlStatement instanceof CreateTableStatement)) {
            return sql;
        }
        String table = ((CreateTableStatement) sqlStatement).getTable().getTableName().getIdentifier().getValue();
        Collection<ColumnDefinitionSegment> encryptColumns = ((CreateTableStatement) sqlStatement).getColumnDefinitions().stream()
                .filter(each -> !each.isPrimaryKey() && !each.isAutoIncrement()).collect(Collectors.toList());
        if (encryptColumns.isEmpty()) {
            return sql;
        }
        context.addEncryptTableRule(new EncryptTableRuleConfiguration(table, getEncryptColumnRuleConfigurations(context.getDatabaseType(), encryptColumns)));
        return sql;
    }
    
    private Collection<EncryptColumnRuleConfiguration> getEncryptColumnRuleConfigurations(final DatabaseType databaseType, final Collection<ColumnDefinitionSegment> encryptColumns) {
        Collection<EncryptColumnRuleConfiguration> result;
        // todo Refactor to DialectDatabaseMetaData spi
        if (DatabaseTypeUtils.isOracleDatabase(databaseType)) {
            result = getEncryptColumnRuleConfig(encryptColumns, true);
        } else {
            result = getEncryptColumnRuleConfig(encryptColumns, false);
        }
        return result;
    }
    
    private static Collection<EncryptColumnRuleConfiguration> getEncryptColumnRuleConfig(final Collection<ColumnDefinitionSegment> encryptColumns, final boolean upperCase) {
        Collection<EncryptColumnRuleConfiguration> result = new LinkedList<>();
        for (ColumnDefinitionSegment each : encryptColumns) {
            result.add(getEncryptColumnRuleConfig(each, upperCase));
        }
        return result;
    }
    
    private static EncryptColumnRuleConfiguration getEncryptColumnRuleConfig(final ColumnDefinitionSegment encryptColumn, final boolean upperCase) {
        IdentifierValue identifier = encryptColumn.getColumnName().getIdentifier();
        boolean unquoted = QuoteCharacter.NONE == identifier.getQuoteCharacter();
        String encryptColumnValue = upperCase && unquoted ? identifier.getValue().toUpperCase() : identifier.getValue();
        EncryptColumnRuleConfiguration result = new EncryptColumnRuleConfiguration(encryptColumnValue,
                new EncryptColumnItemRuleConfiguration(encryptColumnValue + (upperCase && unquoted ? "_CIPHER" : "_cipher"), "aes_encryptor", "varchar(1000)"));
        result.setDataType(encryptColumn.getDataType().getDataTypeName());
        result.setLikeQuery(new EncryptColumnItemRuleConfiguration(encryptColumnValue + (upperCase && unquoted ? "_LIKE" : "_like"), "like_encryptor", "varchar(1000)"));
        result.setOrderQuery(new EncryptColumnItemRuleConfiguration(encryptColumnValue + (upperCase && unquoted ? "_ORDER" : "_order"), "ope_encryptor", "varchar(1000)"));
        return result;
    }
}
