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

package org.apache.shardingsphere.encrypt.rule.column.item;

import com.sphereex.dbplusengine.SphereEx;
import com.sphereex.dbplusengine.SphereEx.Type;
import org.apache.shardingsphere.encrypt.rule.column.EncryptColumn;
import org.apache.shardingsphere.encrypt.spi.EncryptAlgorithm;
import org.apache.shardingsphere.infra.algorithm.core.context.AlgorithmSQLContext;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.apache.shardingsphere.test.matcher.ShardingSphereArgumentVerifyMatchers.deepEq;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class CipherColumnItemTest {
    
    @Test
    void assertEncryptNullValue() {
        assertNull(new CipherColumnItem("foo_col", mock(EncryptAlgorithm.class)).encrypt("foo-db", "foo_schema", "foo_tbl", "foo_col", (Object) null));
    }
    
    @Test
    void assertEncryptSingleValue() {
        EncryptAlgorithm encryptAlgorithm = mock(EncryptAlgorithm.class);
        // SPEX CHANGED: BEGIN
        when(encryptAlgorithm.encrypt(eq("foo_value"), deepEq(new AlgorithmSQLContext("foo_db", "foo_schema", "foo_tbl", "foo_col")), any())).thenReturn("encrypted_foo_value");
        // SPEX CHANGED: END
        CipherColumnItem cipherColumnItem = new CipherColumnItem("foo_col", encryptAlgorithm);
        // SPEX ADDED: BEGIN
        cipherColumnItem.setEncryptColumn(new EncryptColumn("foo_col", new CipherColumnItem("foo_col_cipher", encryptAlgorithm)));
        // SPEX ADDED: END
        assertThat(cipherColumnItem.encrypt("foo_db", "foo_schema", "foo_tbl", "foo_col", "foo_value"), is("encrypted_foo_value"));
    }
    
    @Test
    void assertEncryptMultipleValues() {
        EncryptAlgorithm encryptAlgorithm = mock(EncryptAlgorithm.class);
        // SPEX CHANGED: BEGIN
        when(encryptAlgorithm.encrypt(eq("foo_value"), deepEq(new AlgorithmSQLContext("foo_db", "foo_schema", "foo_tbl", "foo_col")), any())).thenReturn("encrypted_foo_value");
        // SPEX CHANGED: END
        CipherColumnItem cipherColumnItem = new CipherColumnItem("foo_col", encryptAlgorithm);
        // SPEX ADDED: BEGIN
        cipherColumnItem.setEncryptColumn(new EncryptColumn("foo_col", new CipherColumnItem("foo_col_cipher", encryptAlgorithm)));
        // SPEX ADDED: END
        assertThat(cipherColumnItem.encrypt("foo_db", "foo_schema", "foo_tbl", "foo_col", Arrays.asList(null, "foo_value")), is(Arrays.asList(null, "encrypted_foo_value")));
    }
    
    @SphereEx(Type.MODIFY)
    @Test
    void assertDecryptNullValue() {
        assertNull(new CipherColumnItem("foo_col", mock(EncryptAlgorithm.class)).decrypt("foo-db", "foo_schema", "foo_tbl", "foo_col", null, null));
    }
    
    @Test
    void assertDecrypt() {
        EncryptAlgorithm encryptAlgorithm = mock(EncryptAlgorithm.class, RETURNS_DEEP_STUBS);
        // SPEX CHANGED: BEGIN
        when(encryptAlgorithm.decrypt(eq("encrypted_foo_value"), deepEq(new AlgorithmSQLContext("foo_db", "foo_schema", "foo_tbl", "foo_col")), any())).thenReturn("foo_value");
        // SPEX CHANGED: END
        CipherColumnItem cipherColumnItem = new CipherColumnItem("foo_col", encryptAlgorithm);
        // SPEX ADDED: BEGIN
        cipherColumnItem.setEncryptColumn(new EncryptColumn("foo_col", new CipherColumnItem("foo_col_cipher", encryptAlgorithm)));
        // SPEX ADDED: END
        // SPEX CHANGED: BEGIN
        assertThat(cipherColumnItem.decrypt("foo_db", "foo_schema", "foo_tbl", "foo_col", "encrypted_foo_value", null), is("foo_value"));
        // SPEX CHANGED: END
    }
}
