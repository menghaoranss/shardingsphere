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

package com.sphereex.dbplusengine.encrypt.merge.dql.function.type.dialect.mysql;

import com.sphereex.dbplusengine.encrypt.merge.dql.function.EncryptFunctionMergedResultGetter;
import org.apache.shardingsphere.encrypt.rule.EncryptRule;
import org.apache.shardingsphere.encrypt.rule.column.EncryptColumn;
import org.apache.shardingsphere.infra.database.core.type.DatabaseType;
import org.apache.shardingsphere.infra.merge.result.MergedResult;
import org.apache.shardingsphere.sql.parser.statement.core.segment.dml.column.ColumnSegment;
import org.apache.shardingsphere.sql.parser.statement.core.segment.dml.expr.ExpressionSegment;
import org.apache.shardingsphere.sql.parser.statement.core.segment.dml.expr.FunctionSegment;
import org.apache.shardingsphere.sql.parser.statement.core.segment.dml.expr.simple.LiteralExpressionSegment;

import java.sql.SQLException;
import java.util.Iterator;

/**
 * Encrypt if null function merged result getter.
 */
public final class EncryptIfNullFunctionMergedResultGetter implements EncryptFunctionMergedResultGetter {
    
    @Override
    public boolean isNeedDecrypt(final ExpressionSegment expressionSegment) {
        return expressionSegment instanceof FunctionSegment && "IFNULL".equalsIgnoreCase(((FunctionSegment) expressionSegment).getFunctionName())
                && 2 == ((FunctionSegment) expressionSegment).getParameters().size();
    }
    
    @Override
    public Object getValue(final MergedResult mergedResult, final EncryptRule encryptRule, final DatabaseType databaseType,
                           final ExpressionSegment expressionSegment, final int columnIndex, final Class<?> type) throws SQLException {
        FunctionSegment functionSegment = (FunctionSegment) expressionSegment;
        Iterator<ExpressionSegment> paramIterator = functionSegment.getParameters().iterator();
        ExpressionSegment firstParam = paramIterator.next();
        ExpressionSegment secondParam = paramIterator.next();
        if (firstParam instanceof ColumnSegment && secondParam instanceof LiteralExpressionSegment) {
            return getValue(mergedResult, encryptRule, databaseType, columnIndex, type, (ColumnSegment) firstParam);
        }
        return mergedResult.getValue(columnIndex, type);
    }
    
    private Object getValue(final MergedResult mergedResult, final EncryptRule encryptRule, final DatabaseType databaseType,
                            final int columnIndex, final Class<?> type, final ColumnSegment columnSegment) throws SQLException {
        String originalTableName = columnSegment.getColumnBoundInfo().getOriginalTable().getValue();
        String originalColumnName = columnSegment.getColumnBoundInfo().getOriginalColumn().getValue();
        if (!encryptRule.findEncryptTable(originalTableName).map(optional -> optional.isEncryptColumn(originalColumnName)).orElse(false)
                || encryptRule.isQueryWithPlain(originalTableName, originalColumnName)) {
            return mergedResult.getValue(columnIndex, type);
        }
        Object cipherValue = mergedResult.getValue(columnIndex, Object.class);
        EncryptColumn encryptColumn = encryptRule.getEncryptTable(originalTableName).getEncryptColumn(originalColumnName);
        return encryptColumn.getCipher().decrypt(columnSegment.getColumnBoundInfo().getOriginalDatabase().getValue(),
                columnSegment.getColumnBoundInfo().getOriginalSchema().getValue(), originalTableName, originalColumnName, cipherValue, databaseType);
    }
    
    @Override
    public String getType() {
        return "MySQL:IFNULL";
    }
}
