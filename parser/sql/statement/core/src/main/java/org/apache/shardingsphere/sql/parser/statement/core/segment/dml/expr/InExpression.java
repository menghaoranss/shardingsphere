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

package org.apache.shardingsphere.sql.parser.statement.core.segment.dml.expr;

import com.sphereex.dbplusengine.SphereEx;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import org.apache.shardingsphere.sql.parser.statement.core.segment.dml.column.ColumnSegment;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

/**
 * In expression.
 */
@RequiredArgsConstructor
@Getter
@Setter
public final class InExpression implements ExpressionSegment {
    
    private final int startIndex;
    
    private final int stopIndex;
    
    private final ExpressionSegment left;
    
    private final ExpressionSegment right;
    
    private final boolean not;
    
    /**
     * Get expression list from right.
     *
     * @return expression list.
     */
    public Collection<ExpressionSegment> getExpressionList() {
        Collection<ExpressionSegment> result = new LinkedList<>();
        if (right instanceof ListExpression) {
            result.addAll(((ListExpression) right).getItems());
        } else {
            result.add(this);
        }
        return result;
    }
    
    /**
     * Get expression list from right.
     *
     * @param rowExpression row expression
     * @param columnName column name
     * @return expression list
     */
    @SphereEx
    public Collection<ExpressionSegment> getRowExpressionList(final RowExpression rowExpression, final String columnName) {
        Collection<ExpressionSegment> result = new LinkedList<>();
        int columnIndex = getColumnIndex(new ArrayList<>(rowExpression.getItems()), columnName);
        if (!(right instanceof ListExpression)) {
            return Collections.singleton(right);
        }
        for (ExpressionSegment each : ((ListExpression) right).getItems()) {
            if (each instanceof RowExpression) {
                result.add(new ArrayList<>(((RowExpression) each).getItems()).get(columnIndex));
            } else {
                result.add(each);
            }
        }
        return result;
    }
    
    @SphereEx
    private int getColumnIndex(final List<ExpressionSegment> rowItems, final String columnName) {
        int result = 0;
        for (int index = 0; index < rowItems.size(); index++) {
            ExpressionSegment expression = rowItems.get(index);
            if (expression instanceof ColumnSegment && ((ColumnSegment) expression).getIdentifier().getValue().equalsIgnoreCase(columnName)) {
                result = index;
                break;
            }
        }
        return result;
    }
    
    @Override
    public String getText() {
        String operator = not ? " NOT IN " : " IN ";
        return left.getText() + operator + right.getText();
    }
}
