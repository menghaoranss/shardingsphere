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

package org.apache.shardingsphere.infra.rewrite.sql.token.common.pojo.generic;

import com.sphereex.dbplusengine.SphereEx;
import com.sphereex.dbplusengine.SphereEx.Type;
import lombok.Getter;
import org.apache.shardingsphere.infra.exception.core.ShardingSpherePreconditions;
import org.apache.shardingsphere.infra.exception.generic.UnsupportedSQLOperationException;
import org.apache.shardingsphere.sql.parser.statement.core.enums.ParameterMarkerType;
import org.apache.shardingsphere.sql.parser.statement.core.segment.dml.expr.ExpressionSegment;
import org.apache.shardingsphere.sql.parser.statement.core.segment.dml.expr.simple.LiteralExpressionSegment;
import org.apache.shardingsphere.sql.parser.statement.core.segment.dml.expr.simple.ParameterMarkerExpressionSegment;

import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;

/**
 * Insert value.
 */
@Getter
public class InsertValue {
    
    private final List<ExpressionSegment> values;
    
    @SphereEx
    @Getter
    private final Map<Integer, Collection<ExpressionSegment>> addedValues = new HashMap<>();
    
    @SphereEx
    private int addedValuesCount;
    
    @SphereEx
    private int addedParameterMarkerExpressionCount;
    
    public InsertValue(final List<ExpressionSegment> values) {
        ShardingSpherePreconditions.checkNotEmpty(values, () -> new UnsupportedSQLOperationException("Insert values can not be empty"));
        this.values = values;
    }
    
    /**
     * Add added value.
     *
     * @param index index
     * @param value value
     */
    @SphereEx
    public void addAddedValue(final int index, final ExpressionSegment value) {
        addedValuesCount += 1;
        if (value instanceof ParameterMarkerExpressionSegment) {
            addedParameterMarkerExpressionCount++;
        }
        addedValues.computeIfAbsent(index, unused -> new LinkedList<>()).add(value);
    }
    
    /**
     * Get parameter index count.
     *
     * @return parameter index count
     */
    @SphereEx
    public int getParameterIndexCount() {
        int result = 0;
        for (ExpressionSegment each : values) {
            if (each instanceof ParameterMarkerExpressionSegment) {
                result++;
            }
        }
        return result + addedParameterMarkerExpressionCount;
    }
    
    @Override
    public final String toString() {
        StringJoiner result = new StringJoiner(", ", "(", ")");
        for (int i = 0; i < values.size(); i++) {
            result.add(getValue(i));
            // SPEX ADDED: BEGIN
            getAddedValues(i, result);
            // SPEX ADDED: END
        }
        return result.toString();
    }
    
    @SphereEx
    private void getAddedValues(final int index, final StringJoiner joiner) {
        Collection<ExpressionSegment> currentAddedValues = addedValues.get(index);
        if (null != currentAddedValues) {
            for (ExpressionSegment currentAddedValue : currentAddedValues) {
                joiner.add(doGetValue(currentAddedValue));
            }
        }
    }
    
    /**
     * Get value.
     *
     * @param index index
     * @return value
     */
    @SphereEx(Type.MODIFY)
    public String getValue(final int index) {
        ExpressionSegment expressionSegment = values.get(index);
        // SPEX CHANGED: BEGIN
        return doGetValue(expressionSegment);
        // SPEX CHANGED: END
    }
    
    @SphereEx
    private String doGetValue(final ExpressionSegment expressionSegment) {
        if (expressionSegment instanceof ParameterMarkerExpressionSegment) {
            ParameterMarkerExpressionSegment segment = (ParameterMarkerExpressionSegment) expressionSegment;
            return ParameterMarkerType.QUESTION == segment.getParameterMarkerType() ? "?" : "$" + (segment.getParameterMarkerIndex() + 1);
        }
        if (expressionSegment instanceof LiteralExpressionSegment) {
            Object literals = ((LiteralExpressionSegment) expressionSegment).getLiterals();
            return getLiteralValue((LiteralExpressionSegment) expressionSegment, literals);
        }
        return expressionSegment.getText();
    }
    
    private String getLiteralValue(final LiteralExpressionSegment expressionSegment, final Object literals) {
        if (null == literals) {
            return "NULL";
        }
        return literals instanceof String ? "'" + expressionSegment.getLiterals() + "'" : String.valueOf(literals);
    }
}
