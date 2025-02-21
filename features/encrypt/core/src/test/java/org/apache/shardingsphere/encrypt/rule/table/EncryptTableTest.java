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

package org.apache.shardingsphere.encrypt.rule.table;

import com.sphereex.dbplusengine.SphereEx;
import com.sphereex.dbplusengine.encrypt.config.rule.PlainColumnItemRuleConfiguration;
import com.sphereex.dbplusengine.encrypt.rule.mode.EncryptMode;
import com.sphereex.dbplusengine.encrypt.config.rule.mode.EncryptModeType;
import org.apache.shardingsphere.encrypt.config.rule.EncryptColumnItemRuleConfiguration;
import org.apache.shardingsphere.encrypt.config.rule.EncryptColumnRuleConfiguration;
import org.apache.shardingsphere.encrypt.config.rule.EncryptTableRuleConfiguration;
import org.apache.shardingsphere.encrypt.exception.metadata.EncryptLogicColumnNotFoundException;
import org.apache.shardingsphere.encrypt.spi.EncryptAlgorithm;
import org.apache.shardingsphere.infra.database.core.type.DatabaseType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class EncryptTableTest {
    
    private EncryptTable encryptTable;
    
    @BeforeEach
    void setUp() {
        EncryptColumnRuleConfiguration fooColumnRuleConfig = new EncryptColumnRuleConfiguration("foo_col", new EncryptColumnItemRuleConfiguration("foo_col_cipher", "foo_algo"));
        fooColumnRuleConfig.setAssistedQuery(new EncryptColumnItemRuleConfiguration("foo_col_assisted_query", "foo_assist_query_algo"));
        fooColumnRuleConfig.setLikeQuery(new EncryptColumnItemRuleConfiguration("foo_col_like", "foo_like_algo"));
        // SPEX ADDED: BEGIN
        fooColumnRuleConfig.setPlain(new PlainColumnItemRuleConfiguration("foo_col_plain"));
        // SPEX ADDED: END
        EncryptColumnRuleConfiguration barColumnRuleConfig = new EncryptColumnRuleConfiguration("bar_col", new EncryptColumnItemRuleConfiguration("bar_col_cipher", "bar_algo"));
        // SPEX CHANGED: BEGIN
        encryptTable = new EncryptTable(new EncryptTableRuleConfiguration("foo_tbl", Arrays.asList(fooColumnRuleConfig, barColumnRuleConfig)), createEncryptors(),
                mock(DatabaseType.class), mockEncryptMode());
        // SPEX CHANGED: END
    }
    
    private Map<String, EncryptAlgorithm> createEncryptors() {
        Map<String, EncryptAlgorithm> result = new HashMap<>(3, 1F);
        result.put("foo_algo", mock(EncryptAlgorithm.class));
        result.put("foo_assist_query_algo", mock(EncryptAlgorithm.class));
        result.put("foo_like_algo", mock(EncryptAlgorithm.class));
        return result;
    }
    
    @SphereEx
    private EncryptMode mockEncryptMode() {
        EncryptMode result = mock(EncryptMode.class);
        when(result.getType()).thenReturn(EncryptModeType.BACKEND);
        when(result.getRenameTablePrefix()).thenReturn(Optional.of("SPEX_"));
        return result;
    }
    
    @Test
    void assertFindEncryptorName() {
        assertTrue(encryptTable.findEncryptor("foo_col").isPresent());
    }
    
    @Test
    void assertNotFindEncryptorName() {
        assertFalse(encryptTable.findEncryptor("no_col").isPresent());
    }
    
    @Test
    void assertIsEncryptColumn() {
        assertTrue(encryptTable.isEncryptColumn("foo_col"));
    }
    
    @Test
    void assertGetEncryptColumn() {
        assertNotNull(encryptTable.getEncryptColumn("foo_col"));
    }
    
    @Test
    void assertIsCipherColumn() {
        assertTrue(encryptTable.isCipherColumn("foo_col_cipher"));
    }
    
    @Test
    void assertIsNotCipherColumn() {
        assertFalse(encryptTable.isCipherColumn("foo_col"));
    }
    
    @Test
    void assertGetLogicColumnByCipherColumn() {
        assertThat(encryptTable.getLogicColumnByCipherColumn("foo_col_cipher"), is("foo_col"));
    }
    
    @Test
    void assertGetLogicColumnByCipherColumnWhenNotFind() {
        assertThrows(EncryptLogicColumnNotFoundException.class, () -> encryptTable.getLogicColumnByCipherColumn("no_col"));
    }
    
    @Test
    void assertGetLogicColumnByAssistedQueryColumn() {
        assertThat(encryptTable.getLogicColumnByAssistedQueryColumn("foo_col_assisted_query"), is("foo_col"));
    }
    
    @Test
    void assertGetLogicColumnByAssistedQueryColumnWhenNotFind() {
        assertThrows(EncryptLogicColumnNotFoundException.class, () -> encryptTable.getLogicColumnByAssistedQueryColumn("no_col"));
    }
    
    @Test
    void assertIsAssistedQueryColumn() {
        assertTrue(encryptTable.isAssistedQueryColumn("foo_col_assisted_query"));
    }
    
    @Test
    void assertIsLikeColumn() {
        assertTrue(encryptTable.isLikeQueryColumn("foo_col_like"));
    }
    
    @Test
    void assertFindQueryEncryptor() {
        assertTrue(encryptTable.getEncryptColumn("foo_col").getAssistedQuery().isPresent());
        assertThat(encryptTable.findQueryEncryptor("foo_col"), is(Optional.of(encryptTable.getEncryptColumn("foo_col").getAssistedQuery().get().getEncryptor())));
    }
    
    @Test
    void assertFindQueryEncryptorWithoutEncryptColumn() {
        assertThat(encryptTable.findQueryEncryptor("no_col"), is(Optional.empty()));
    }
    
    @SphereEx
    @Test
    void assertGetLogicColumnByPlainColumn() {
        assertThat(encryptTable.getLogicColumnByPlainColumn("foo_col_plain"), is("foo_col"));
    }
    
    @SphereEx
    @Test
    void assertGetLogicColumnByPlainColumnWhenNotFind() {
        assertThrows(EncryptLogicColumnNotFoundException.class, () -> encryptTable.getLogicColumnByPlainColumn("invalidColumn"));
    }
    
    @SphereEx
    @Test
    void assertIsQueryWithPlain() {
        boolean actual = encryptTable.isQueryWithPlain("foo_col");
        assertFalse(actual);
        EncryptColumnRuleConfiguration columnRuleConfig = new EncryptColumnRuleConfiguration("foo_col", new EncryptColumnItemRuleConfiguration("foo_col_cipher", "foo_algo"));
        columnRuleConfig.setAssistedQuery(new EncryptColumnItemRuleConfiguration("foo_col_assisted_query", "foo_assist_query_algo"));
        columnRuleConfig.setLikeQuery(new EncryptColumnItemRuleConfiguration("foo_col_like", "foo_like_algo"));
        PlainColumnItemRuleConfiguration plainConfig = new PlainColumnItemRuleConfiguration("foo_col_plain");
        plainConfig.setQueryWithPlain(true);
        columnRuleConfig.setPlain(plainConfig);
        EncryptTableRuleConfiguration encryptTableRuleConfig = new EncryptTableRuleConfiguration("foo_tbl", Collections.singleton(columnRuleConfig));
        encryptTable = new EncryptTable(encryptTableRuleConfig, Collections.singletonMap("foo_algo", mock(EncryptAlgorithm.class)), mock(DatabaseType.class), mock(EncryptMode.class));
        actual = encryptTable.isQueryWithPlain("foo_col");
        assertTrue(actual);
    }
    
    @SphereEx
    @Test
    void assertGetLogicColumns() {
        assertThat(encryptTable.getLogicColumns(), is(new HashSet<>(Arrays.asList("foo_col", "bar_col"))));
    }
    
    @SphereEx
    @Test
    void assertGetRenameTableWithNotConfig() {
        assertTrue(encryptTable.getRenameTable().isPresent());
        assertThat(encryptTable.getRenameTable().get(), is("SPEX_foo_tbl"));
    }
    
    @SphereEx
    @Test
    void assertIsAllCipherColumnConfigPlain() {
        EncryptColumnRuleConfiguration columnRuleConfig = new EncryptColumnRuleConfiguration("foo_col", new EncryptColumnItemRuleConfiguration("foo_col_cipher", "foo_algo"));
        columnRuleConfig.setPlain(new PlainColumnItemRuleConfiguration("foo_col_plain"));
        EncryptTable encryptTable = new EncryptTable(new EncryptTableRuleConfiguration("foo_tbl", Collections.singleton(columnRuleConfig)), createEncryptors(),
                mock(DatabaseType.class), mockEncryptMode());
        assertTrue(encryptTable.isAllCipherColumnConfigPlain());
    }
    
    @SphereEx
    @Test
    void assertIsNotAllCipherColumnConfigPlain() {
        assertFalse(encryptTable.isAllCipherColumnConfigPlain());
    }
}
