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

package org.apache.shardingsphere.encrypt.metadata;

import com.sphereex.dbplusengine.SphereEx;
import com.sphereex.dbplusengine.SphereEx.Type;
import org.apache.shardingsphere.encrypt.rule.EncryptRule;
import org.apache.shardingsphere.encrypt.rule.column.EncryptColumn;
import org.apache.shardingsphere.encrypt.rule.table.EncryptTable;
import org.apache.shardingsphere.infra.database.core.metadata.data.model.ColumnMetaData;
import org.apache.shardingsphere.infra.database.core.metadata.data.model.SchemaMetaData;
import org.apache.shardingsphere.infra.database.core.metadata.data.model.TableMetaData;
import org.apache.shardingsphere.infra.database.core.metadata.database.datatype.DataTypeRegistry;
import org.apache.shardingsphere.infra.database.core.type.DatabaseType;
import org.apache.shardingsphere.infra.metadata.database.resource.unit.StorageUnit;
import org.apache.shardingsphere.infra.metadata.database.schema.builder.GenericSchemaBuilderMaterial;
import org.apache.shardingsphere.infra.metadata.database.schema.model.ShardingSphereColumn;
import org.apache.shardingsphere.infra.metadata.database.schema.model.ShardingSphereSchema;
import org.apache.shardingsphere.infra.metadata.database.schema.model.ShardingSphereTable;
import org.apache.shardingsphere.infra.metadata.database.schema.reviser.MetaDataReviseEngine;
import org.apache.shardingsphere.infra.spi.type.typed.TypedSPILoader;
import org.apache.shardingsphere.test.mock.AutoMockExtension;
import org.apache.shardingsphere.test.mock.StaticMockSettings;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.matches;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

// SPEX ADDED: BEGIN
@ExtendWith(AutoMockExtension.class)
@StaticMockSettings(DataTypeRegistry.class)
@MockitoSettings(strictness = Strictness.LENIENT)
// SPEX ADDED: END
class EncryptMetaDataReviseEngineTest {
    
    @Test
    void assertRevise() {
        Map<String, SchemaMetaData> schemaMetaData = Collections.singletonMap("foo_db", new SchemaMetaData("foo_db", Collections.singleton(createTableMetaData())));
        Map<String, ShardingSphereSchema> actual = new MetaDataReviseEngine(Collections.singleton(mockEncryptRule())).revise(schemaMetaData, mock(GenericSchemaBuilderMaterial.class));
        assertThat(actual.size(), is(1));
        assertTrue(actual.containsKey("foo_db"));
        assertThat(actual.get("foo_db").getAllTables().size(), is(1));
        ShardingSphereTable table = actual.get("foo_db").getAllTables().iterator().next();
        assertThat(table.getAllColumns().size(), is(2));
        List<ShardingSphereColumn> columns = new ArrayList<>(table.getAllColumns());
        assertThat(columns.get(0).getName(), is("id"));
        assertThat(columns.get(1).getName(), is("pwd"));
    }
    
    private EncryptRule mockEncryptRule() {
        EncryptRule result = mock(EncryptRule.class, RETURNS_DEEP_STUBS);
        EncryptTable encryptTable = mock(EncryptTable.class);
        when(result.findEncryptTable("foo_tbl")).thenReturn(Optional.of(encryptTable));
        when(encryptTable.isCipherColumn("pwd_cipher")).thenReturn(true);
        when(encryptTable.isLikeQueryColumn("pwd_like")).thenReturn(true);
        when(encryptTable.getLogicColumnByCipherColumn("pwd_cipher")).thenReturn("pwd");
        // SPEX ADDED: BEGIN
        when(encryptTable.isPlainColumn("pwd_plain")).thenReturn(true);
        when(encryptTable.getLogicColumnByPlainColumn("pwd_plain")).thenReturn("pwd");
        EncryptColumn encryptColumn = mock(EncryptColumn.class);
        when(encryptTable.getEncryptColumn("pwd")).thenReturn(encryptColumn);
        when(result.findEncryptTableByActualTable("foo_tbl")).thenReturn(Optional.empty());
        // SPEX ADDED: END
        return result;
    }
    
    @SphereEx(Type.MODIFY)
    private TableMetaData createTableMetaData() {
        Collection<ColumnMetaData> columns = Arrays.asList(new ColumnMetaData("id", Types.INTEGER, true, true, true, true, false, false, ""),
                new ColumnMetaData("pwd_cipher", Types.VARCHAR, false, false, true, true, false, false, ""),
                new ColumnMetaData("pwd_plain", Types.VARCHAR, false, false, true, true, false, false, ""),
                new ColumnMetaData("pwd_like", Types.VARCHAR, false, false, true, true, false, false, ""));
        return new TableMetaData("foo_tbl", columns, Collections.emptyList(), Collections.emptyList());
    }
    
    @SphereEx
    @Test
    void assertDecorateWithConfiguredDataType() {
        GenericSchemaBuilderMaterial materials = mock(GenericSchemaBuilderMaterial.class);
        DatabaseType databaseType = TypedSPILoader.getService(DatabaseType.class, "FIXTURE");
        StorageUnit storageUnit = mock(StorageUnit.class, RETURNS_DEEP_STUBS);
        when(storageUnit.getStorageType()).thenReturn(databaseType);
        when(materials.getStorageUnits()).thenReturn(Collections.singletonMap("logic_db", storageUnit));
        when(DataTypeRegistry.getDataType(eq(databaseType.getType()), matches(Pattern.compile("integer", Pattern.CASE_INSENSITIVE)))).thenReturn(Optional.of(Types.INTEGER));
        Map<String, ShardingSphereSchema> actual = new MetaDataReviseEngine(Collections.singleton(mockEncryptRuleWithDataTypeConfiguration()))
                .revise(Collections.singletonMap("logic_db", new SchemaMetaData("logic_db", Collections.singleton(createTableMetaData()))), materials);
        assertThat(actual.size(), is(1));
        assertTrue(actual.containsKey("logic_db"));
        assertThat(actual.get("logic_db").getAllTables().size(), is(1));
        ShardingSphereTable table = actual.get("logic_db").getAllTables().iterator().next();
        assertThat(table.getAllColumns().size(), is(2));
        Iterator<ShardingSphereColumn> columns = table.getAllColumns().iterator();
        assertThat(columns.next().getName(), is("id"));
        ShardingSphereColumn column = columns.next();
        assertThat(column.getName(), is("pwd"));
        assertThat(column.getDataType(), is(Types.INTEGER));
    }
    
    @SphereEx
    private EncryptRule mockEncryptRuleWithDataTypeConfiguration() {
        EncryptRule result = mockEncryptRule();
        assertTrue(result.findEncryptTable("foo_tbl").isPresent());
        EncryptTable encryptTable = result.findEncryptTable("foo_tbl").get();
        when(encryptTable.isCipherColumn("pwd")).thenReturn(true);
        when(encryptTable.getLogicColumnByCipherColumn("pwd")).thenReturn("pwd");
        EncryptColumn encryptColumn = mock(EncryptColumn.class);
        when(encryptColumn.getDataType()).thenReturn("INTEGER");
        when(encryptTable.getEncryptColumn("pwd")).thenReturn(encryptColumn);
        return result;
    }
}
