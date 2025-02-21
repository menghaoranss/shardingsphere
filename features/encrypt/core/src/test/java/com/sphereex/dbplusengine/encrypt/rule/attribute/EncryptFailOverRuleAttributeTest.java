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

package com.sphereex.dbplusengine.encrypt.rule.attribute;

import org.apache.shardingsphere.encrypt.rule.EncryptRule;
import org.apache.shardingsphere.infra.metadata.database.resource.ResourceMetaData;
import org.apache.shardingsphere.sql.parser.statement.core.segment.generic.table.SimpleTableSegment;
import org.apache.shardingsphere.sql.parser.statement.core.segment.generic.table.TableNameSegment;
import org.apache.shardingsphere.sql.parser.statement.core.statement.dml.InsertStatement;
import org.apache.shardingsphere.sql.parser.statement.core.statement.dml.SelectStatement;
import org.apache.shardingsphere.sql.parser.statement.core.value.identifier.IdentifierValue;
import org.apache.shardingsphere.test.mock.AutoMockExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import javax.sql.DataSource;
import java.util.Collections;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(AutoMockExtension.class)
class EncryptFailOverRuleAttributeTest {
    
    @Test
    void assertIsNeedFailOverWhenNotConfigUseOriginalSQLWhenCipherQueryFailed() {
        EncryptRule encryptRule = mock(EncryptRule.class, RETURNS_DEEP_STUBS);
        when(encryptRule.getEncryptMode().isUseOriginalSQLWhenCipherQueryFailed()).thenReturn(false);
        assertFalse(new EncryptFailOverRuleAttribute("foo_db", encryptRule).isNeedFailOver(mock(SelectStatement.class)));
    }
    
    @Test
    void assertIsNeedFailOverWhenNotSelectStatement() {
        EncryptRule encryptRule = mock(EncryptRule.class, RETURNS_DEEP_STUBS);
        when(encryptRule.getEncryptMode().isUseOriginalSQLWhenCipherQueryFailed()).thenReturn(true);
        assertFalse(new EncryptFailOverRuleAttribute("foo_db", encryptRule).isNeedFailOver(mock(InsertStatement.class)));
    }
    
    @Test
    void assertIsNeedFailOverWhenSelectStatementNotContainsTable() {
        EncryptRule encryptRule = mock(EncryptRule.class, RETURNS_DEEP_STUBS);
        when(encryptRule.getEncryptMode().isUseOriginalSQLWhenCipherQueryFailed()).thenReturn(true);
        assertTrue(new EncryptFailOverRuleAttribute("foo_db", encryptRule).isNeedFailOver(mock(SelectStatement.class)));
    }
    
    @Test
    void assertIsNeedFailOverWhenSelectStatementNotAllCipherColumnConfigPlain() {
        EncryptRule encryptRule = mock(EncryptRule.class, RETURNS_DEEP_STUBS);
        when(encryptRule.getEncryptMode().isUseOriginalSQLWhenCipherQueryFailed()).thenReturn(true);
        SelectStatement sqlStatement = mock(SelectStatement.class, RETURNS_DEEP_STUBS);
        when(sqlStatement.getFrom().isPresent()).thenReturn(true);
        when(sqlStatement.getFrom().get()).thenReturn(new SimpleTableSegment(new TableNameSegment(0, 0, new IdentifierValue("t_user"))));
        when(encryptRule.findEncryptTable("t_user").isPresent()).thenReturn(true);
        assertFalse(new EncryptFailOverRuleAttribute("foo_db", encryptRule).isNeedFailOver(sqlStatement));
    }
    
    @Test
    void assertIsNeedFailOverWhenSelectStatementAllCipherColumnConfigPlain() {
        EncryptRule encryptRule = mock(EncryptRule.class, RETURNS_DEEP_STUBS);
        when(encryptRule.getEncryptMode().isUseOriginalSQLWhenCipherQueryFailed()).thenReturn(true);
        SelectStatement sqlStatement = mock(SelectStatement.class, RETURNS_DEEP_STUBS);
        when(sqlStatement.getFrom().isPresent()).thenReturn(true);
        when(sqlStatement.getFrom().get()).thenReturn(new SimpleTableSegment(new TableNameSegment(0, 0, new IdentifierValue("t_user"))));
        when(encryptRule.findEncryptTable("t_user").isPresent()).thenReturn(true);
        when(encryptRule.getEncryptTable("t_user").isAllCipherColumnConfigPlain()).thenReturn(true);
        assertTrue(new EncryptFailOverRuleAttribute("foo_db", encryptRule).isNeedFailOver(sqlStatement));
    }
    
    @Test
    void assertGetFailOverDataSourceName() {
        EncryptRule encryptRule = mock(EncryptRule.class, RETURNS_DEEP_STUBS);
        ResourceMetaData resourceMetaData = mock(ResourceMetaData.class, RETURNS_DEEP_STUBS);
        when(resourceMetaData.getDataSourceMap()).thenReturn(Collections.singletonMap("foo_db", mock(DataSource.class)));
        assertThat(new EncryptFailOverRuleAttribute("foo_db", encryptRule).getFailOverDataSourceName(resourceMetaData), is("foo_db"));
    }
}
