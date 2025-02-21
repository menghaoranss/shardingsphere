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

package com.sphereex.dbplusengine.test.it.rewrite.util;

import com.google.common.base.Strings;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.SneakyThrows;
import org.apache.shardingsphere.infra.database.core.type.DatabaseType;
import org.apache.shardingsphere.infra.parser.sql.SQLStatementParserEngine;
import org.apache.shardingsphere.infra.spi.type.typed.TypedSPILoader;
import org.apache.shardingsphere.parser.config.SQLParserRuleConfiguration;
import org.apache.shardingsphere.parser.rule.builder.DefaultSQLParserRuleConfigurationBuilder;
import org.apache.shardingsphere.sql.parser.statement.core.segment.ddl.column.ColumnDefinitionSegment;
import org.apache.shardingsphere.sql.parser.statement.core.statement.SQLStatement;
import org.apache.shardingsphere.sql.parser.statement.core.statement.ddl.CreateTableStatement;
import org.h2.util.ScriptReader;
import org.h2.util.StringUtils;

import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Table column extract utils.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class TableColumnExtractUtils {
    
    /**
     * Extract table and columns.
     *
     * @param filePath file path
     * @param databaseType database type
     *
     * @return table and columns
     */
    public static Map<String, Collection<ColumnDefinitionSegment>> extractTableColumns(final String filePath, final String databaseType) {
        SQLParserRuleConfiguration sqlParserRuleConfig = new DefaultSQLParserRuleConfigurationBuilder().build();
        SQLStatementParserEngine parserEngine =
                new SQLStatementParserEngine(TypedSPILoader.getService(DatabaseType.class, databaseType), sqlParserRuleConfig.getSqlStatementCache(), sqlParserRuleConfig.getParseTreeCache());
        Map<String, Collection<ColumnDefinitionSegment>> result = new LinkedHashMap<>();
        for (String each : getSQLs(filePath)) {
            String sql = replaceComment(each);
            if (Strings.isNullOrEmpty(sql.trim())) {
                continue;
            }
            SQLStatement sqlStatement = parserEngine.parse(sql, false);
            if (!(sqlStatement instanceof CreateTableStatement)) {
                continue;
            }
            CreateTableStatement createTableSQLStatement = (CreateTableStatement) sqlStatement;
            result.put(createTableSQLStatement.getTable().getTableName().getIdentifier().getValue(), createTableSQLStatement.getColumnDefinitions());
        }
        return result;
    }
    
    private static String replaceComment(final String sql) {
        String result = sql;
        if (sql.trim().startsWith("--")) {
            result = sql.replaceAll("-+.*" + System.lineSeparator() + "*", "");
        }
        return result;
    }
    
    @SneakyThrows(IOException.class)
    private static Collection<String> getSQLs(final String filePath) {
        Collection<String> result = new ArrayList<>();
        try (ScriptReader reader = new ScriptReader(new FileReader(filePath))) {
            String sql;
            do {
                sql = reader.readStatement();
                Optional.ofNullable(sql).map(String::trim).map(optional -> optional.endsWith(";") ? optional.substring(0, optional.length() - 1) : optional).filter(each -> !each.isEmpty())
                        .ifPresent(result::add);
            } while (!StringUtils.isNullOrEmpty(sql));
        }
        return result;
    }
}
