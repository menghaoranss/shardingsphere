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

import org.apache.shardingsphere.sql.parser.statement.core.segment.dml.column.ColumnSegment;
import org.apache.shardingsphere.sql.parser.statement.core.segment.dml.expr.ExpressionSegment;
import org.apache.shardingsphere.sql.parser.statement.core.segment.dml.expr.simple.LiteralExpressionSegment;
import org.apache.shardingsphere.sql.parser.statement.core.segment.dml.expr.simple.ParameterMarkerExpressionSegment;
import org.apache.shardingsphere.sql.parser.statement.core.value.identifier.IdentifierValue;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

class EncryptBetweenConditionTest {
    
    @Test
    void assertNewInstance() {
        ColumnSegment columnSegment = new ColumnSegment(0, 0, new IdentifierValue("foo_col"));
        EncryptBetweenCondition actual = new EncryptBetweenCondition(columnSegment, "foo_tbl", new ParameterMarkerExpressionSegment(0, 0, 0), new LiteralExpressionSegment(0, 0, "foo"), true);
        assertThat(actual.getPositionIndexMap(), is(Collections.singletonMap(0, 0)));
        assertThat(actual.getPositionValueMap(), is(Collections.singletonMap(1, "foo")));
    }
    
    @Test
    void assertNewInstanceWithUnsupportedExpressionSegment() {
        ColumnSegment columnSegment = new ColumnSegment(0, 0, new IdentifierValue("foo_col"));
        EncryptBetweenCondition actual = new EncryptBetweenCondition(columnSegment, "foo_tbl", mock(ExpressionSegment.class), mock(ExpressionSegment.class), true);
        assertTrue(actual.getPositionIndexMap().isEmpty());
        assertTrue(actual.getPositionValueMap().isEmpty());
    }
}
