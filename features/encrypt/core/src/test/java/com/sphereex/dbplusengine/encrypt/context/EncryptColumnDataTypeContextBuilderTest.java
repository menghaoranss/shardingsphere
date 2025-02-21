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

package com.sphereex.dbplusengine.encrypt.context;

import com.sphereex.dbplusengine.encrypt.rule.column.item.PlainColumnItem;
import org.apache.shardingsphere.encrypt.rule.column.EncryptColumn;
import org.apache.shardingsphere.encrypt.rule.column.item.AssistedQueryColumnItem;
import org.apache.shardingsphere.encrypt.rule.column.item.CipherColumnItem;
import org.apache.shardingsphere.encrypt.spi.EncryptAlgorithm;
import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;

class EncryptColumnDataTypeContextBuilderTest {
    
    @Test
    void assertBuild() {
        EncryptColumnDataTypeContext actual = EncryptColumnDataTypeContextBuilder.build(createEncryptColumn());
        assertThat(actual.getLogicDataType(), is("int(20) unsigned not null default 0"));
        assertThat(actual.getCipherDataType(), is("varchar(200) not null default ''"));
        assertThat(actual.getAssistedQueryDataType(), is("varchar(200) not null"));
        assertNull(actual.getLikeQueryDataType());
        assertNull(actual.getOrderQueryDataType());
    }
    
    private EncryptColumn createEncryptColumn() {
        CipherColumnItem cipherColumn = new CipherColumnItem("cipher_certificate_number", mock(EncryptAlgorithm.class));
        cipherColumn.setDataType("varchar(200) not null default ''");
        EncryptColumn result = new EncryptColumn("certificate_number", cipherColumn);
        cipherColumn.setEncryptColumn(result);
        result.setDataType("int(20) unsigned not null default 0");
        AssistedQueryColumnItem assistedQueryColumn = new AssistedQueryColumnItem("assisted_certificate_number", mock(EncryptAlgorithm.class));
        assistedQueryColumn.setDataType("varchar(200) not null");
        assistedQueryColumn.setEncryptColumn(result);
        result.setAssistedQuery(assistedQueryColumn);
        result.setPlain(new PlainColumnItem("certificate_number_plain", false));
        return result;
    }
}
