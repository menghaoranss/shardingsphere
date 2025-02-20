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

package com.sphereex.dbplusengine.encrypt.rewrite.condition.impl;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.apache.shardingsphere.encrypt.rewrite.condition.EncryptCondition;
import org.apache.shardingsphere.sql.parser.statement.core.segment.dml.column.ColumnSegment;
import org.apache.shardingsphere.sql.parser.statement.core.segment.dml.expr.ExpressionSegment;
import org.apache.shardingsphere.sql.parser.statement.core.segment.dml.expr.simple.LiteralExpressionSegment;
import org.apache.shardingsphere.sql.parser.statement.core.segment.dml.expr.simple.ParameterMarkerExpressionSegment;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Encrypt condition for between.
 */
@Getter
@EqualsAndHashCode
@ToString
public final class EncryptBetweenCondition implements EncryptCondition {
    
    private final ColumnSegment columnSegment;
    
    private final String tableName;
    
    private final int startIndex;
    
    private final int stopIndex;
    
    private final ExpressionSegment betweenExpr;
    
    private final ExpressionSegment andExpr;
    
    private final boolean not;
    
    private final Map<Integer, Integer> positionIndexMap = new LinkedHashMap<>();
    
    private final Map<Integer, Object> positionValueMap = new LinkedHashMap<>();
    
    public EncryptBetweenCondition(final ColumnSegment columnSegment, final String tableName,
                                   final ExpressionSegment betweenExpr, final ExpressionSegment andExpr, final boolean not) {
        this.columnSegment = columnSegment;
        this.tableName = tableName;
        startIndex = betweenExpr.getStartIndex();
        stopIndex = andExpr.getStopIndex();
        this.betweenExpr = betweenExpr;
        this.andExpr = andExpr;
        this.not = not;
        putPositionMap(0, betweenExpr);
        putPositionMap(1, andExpr);
    }
    
    private void putPositionMap(final int index, final ExpressionSegment expressionSegment) {
        if (expressionSegment instanceof ParameterMarkerExpressionSegment) {
            positionIndexMap.put(index, ((ParameterMarkerExpressionSegment) expressionSegment).getParameterMarkerIndex());
        } else if (expressionSegment instanceof LiteralExpressionSegment) {
            positionValueMap.put(index, ((LiteralExpressionSegment) expressionSegment).getLiterals());
        }
    }
}
