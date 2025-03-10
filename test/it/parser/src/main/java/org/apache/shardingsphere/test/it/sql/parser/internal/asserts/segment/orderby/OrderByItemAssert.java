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

package org.apache.shardingsphere.test.it.sql.parser.internal.asserts.segment.orderby;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.apache.shardingsphere.sql.parser.statement.core.segment.dml.order.item.ColumnOrderByItemSegment;
import org.apache.shardingsphere.sql.parser.statement.core.segment.dml.order.item.ExpressionOrderByItemSegment;
import org.apache.shardingsphere.sql.parser.statement.core.segment.dml.order.item.IndexOrderByItemSegment;
import org.apache.shardingsphere.sql.parser.statement.core.segment.dml.order.item.OrderByItemSegment;
import org.apache.shardingsphere.test.it.sql.parser.internal.asserts.SQLCaseAssertContext;
import org.apache.shardingsphere.test.it.sql.parser.internal.asserts.segment.SQLSegmentAssert;
import org.apache.shardingsphere.test.it.sql.parser.internal.asserts.segment.bound.ColumnBoundAssert;
import org.apache.shardingsphere.test.it.sql.parser.internal.asserts.segment.expression.ExpressionAssert;
import org.apache.shardingsphere.test.it.sql.parser.internal.asserts.segment.identifier.IdentifierValueAssert;
import org.apache.shardingsphere.test.it.sql.parser.internal.asserts.segment.owner.OwnerAssert;
import org.apache.shardingsphere.test.it.sql.parser.internal.cases.parser.jaxb.segment.impl.orderby.ExpectedOrderByClause;
import org.apache.shardingsphere.test.it.sql.parser.internal.cases.parser.jaxb.segment.impl.orderby.item.ExpectedOrderByItem;
import org.apache.shardingsphere.test.it.sql.parser.internal.cases.parser.jaxb.segment.impl.orderby.item.impl.ExpectedColumnOrderByItem;
import org.apache.shardingsphere.test.it.sql.parser.internal.cases.parser.jaxb.segment.impl.orderby.item.impl.ExpectedExpressionOrderByItem;
import org.apache.shardingsphere.test.it.sql.parser.internal.cases.parser.jaxb.segment.impl.orderby.item.impl.ExpectedIndexOrderByItem;

import java.util.Collection;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Order by item assert.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class OrderByItemAssert {
    
    /**
     * Assert actual order by segment is correct with expected order by.
     *
     * @param assertContext assert context
     * @param actual actual order by segments
     * @param expected expected order by
     * @param type type of assertion, should be order by or group by
     */
    public static void assertIs(final SQLCaseAssertContext assertContext, final Collection<OrderByItemSegment> actual, final ExpectedOrderByClause expected, final String type) {
        assertThat(assertContext.getText(String.format("%s items size assertion error: ", type)), actual.size(), is(expected.getItemSize()));
        assertColumnOrderByItems(assertContext, actual, expected, type);
        assertIndexOrderByItems(assertContext, actual, expected, type);
        assertExpressionOrderByItems(assertContext, actual, expected, type);
    }
    
    private static void assertColumnOrderByItems(final SQLCaseAssertContext assertContext, final Collection<OrderByItemSegment> actual, final ExpectedOrderByClause expected, final String type) {
        int count = 0;
        for (OrderByItemSegment each : actual) {
            if (each instanceof ColumnOrderByItemSegment) {
                assertOrderInfo(assertContext, each, expected.getColumnItems().get(count), type);
                assertColumnOrderByItem(assertContext, (ColumnOrderByItemSegment) each, expected.getColumnItems().get(count), type);
                count++;
            }
        }
    }
    
    private static void assertIndexOrderByItems(final SQLCaseAssertContext assertContext, final Collection<OrderByItemSegment> actual, final ExpectedOrderByClause expected, final String type) {
        int count = 0;
        for (OrderByItemSegment each : actual) {
            if (each instanceof IndexOrderByItemSegment) {
                assertOrderInfo(assertContext, each, expected.getIndexItems().get(count), type);
                assertIndexOrderByItem(assertContext, (IndexOrderByItemSegment) each, expected.getIndexItems().get(count), type);
                count++;
            }
        }
    }
    
    private static void assertExpressionOrderByItems(final SQLCaseAssertContext assertContext, final Collection<OrderByItemSegment> actual, final ExpectedOrderByClause expected, final String type) {
        int count = 0;
        for (OrderByItemSegment each : actual) {
            if (each instanceof ExpressionOrderByItemSegment) {
                assertOrderInfo(assertContext, each, expected.getExpressionItems().get(count), type);
                assertExpressionOrderByItem(assertContext, (ExpressionOrderByItemSegment) each, expected.getExpressionItems().get(count), type);
                count++;
            }
        }
    }
    
    private static void assertOrderInfo(final SQLCaseAssertContext assertContext, final OrderByItemSegment actual, final ExpectedOrderByItem expected, final String type) {
        assertThat(assertContext.getText(String.format("%s item order direction assertion error: ", type)), actual.getOrderDirection().name(), is(expected.getOrderDirection()));
        if (null != expected.getNullsOrderType()) {
            assertTrue(actual.getNullsOrderType().isPresent(), assertContext.getText("Actual nulls order type should exist."));
            assertThat(assertContext.getText(String.format("%s item nulls order type assertion error: ", type)), actual.getNullsOrderType().get().name(), is(expected.getNullsOrderType()));
        }
    }
    
    private static void assertColumnOrderByItem(final SQLCaseAssertContext assertContext,
                                                final ColumnOrderByItemSegment actual, final ExpectedColumnOrderByItem expected, final String type) {
        IdentifierValueAssert.assertIs(assertContext, actual.getColumn().getIdentifier(), expected, String.format("%s item", type));
        ColumnBoundAssert.assertIs(assertContext, actual.getColumn().getColumnBoundInfo(), expected.getColumnBound());
        if (null == expected.getOwner()) {
            assertFalse(actual.getColumn().getOwner().isPresent(), assertContext.getText("Actual owner should not exist."));
        } else {
            assertTrue(actual.getColumn().getOwner().isPresent(), assertContext.getText("Actual owner should exist."));
            OwnerAssert.assertIs(assertContext, actual.getColumn().getOwner().get(), expected.getOwner());
        }
        SQLSegmentAssert.assertIs(assertContext, actual, expected);
    }
    
    private static void assertIndexOrderByItem(final SQLCaseAssertContext assertContext,
                                               final IndexOrderByItemSegment actual, final ExpectedIndexOrderByItem expected, final String type) {
        assertThat(assertContext.getText(String.format("%s item index assertion error: ", type)), actual.getColumnIndex(), is(expected.getIndex()));
        SQLSegmentAssert.assertIs(assertContext, actual, expected);
    }
    
    private static void assertExpressionOrderByItem(final SQLCaseAssertContext assertContext,
                                                    final ExpressionOrderByItemSegment actual, final ExpectedExpressionOrderByItem expected, final String type) {
        assertThat(assertContext.getText(String.format("%s item expression assertion error: ", type)), actual.getExpression(), is(expected.getExpression()));
        if (null != expected.getExpr()) {
            ExpressionAssert.assertExpression(assertContext, actual.getExpr(), expected.getExpr());
        }
        SQLSegmentAssert.assertIs(assertContext, actual, expected);
    }
}
