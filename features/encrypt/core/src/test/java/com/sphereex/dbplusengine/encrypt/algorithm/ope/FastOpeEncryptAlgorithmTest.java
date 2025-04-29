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

package com.sphereex.dbplusengine.encrypt.algorithm.ope;

import com.sphereex.dbplusengine.encrypt.context.EncryptColumnDataTypeContext;
import com.sphereex.dbplusengine.encrypt.context.EncryptContext;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.shardingsphere.encrypt.spi.EncryptAlgorithm;
import org.apache.shardingsphere.infra.algorithm.core.context.AlgorithmSQLContext;
import org.apache.shardingsphere.infra.database.core.metadata.database.datatype.DataTypeRegistry;
import org.apache.shardingsphere.infra.database.core.type.DatabaseType;
import org.apache.shardingsphere.infra.database.mysql.type.MySQLDatabaseType;
import org.apache.shardingsphere.infra.database.oracle.type.OracleDatabaseType;
import org.apache.shardingsphere.infra.spi.type.typed.TypedSPILoader;
import org.apache.shardingsphere.test.mock.AutoMockExtension;
import org.apache.shardingsphere.test.mock.StaticMockSettings;
import org.apache.shardingsphere.test.util.PropertiesBuilder;
import org.apache.shardingsphere.test.util.PropertiesBuilder.Property;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.sql.Types;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@Slf4j
@ExtendWith(AutoMockExtension.class)
@StaticMockSettings(DataTypeRegistry.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class FastOpeEncryptAlgorithmTest {
    
    private final DatabaseType databaseType = TypedSPILoader.getService(DatabaseType.class, "FIXTURE");
    
    private EncryptAlgorithm encryptAlgorithm;
    
    private EncryptAlgorithm randomPropsEncryptAlgorithm;
    
    @BeforeEach
    void setUp() {
        encryptAlgorithm = TypedSPILoader.getService(EncryptAlgorithm.class, "SphereEx:FASTOPE",
                PropertiesBuilder.build(new Property("alpha-key", "0.8935217796678353"), new Property("factor-e-key", "0.9186430364852792"), new Property("factor-k-key", "523211953918290")));
        double alphaKey = ThreadLocalRandom.current().nextDouble(0.8, 1);
        double factorEKey = ThreadLocalRandom.current().nextDouble(0.012, 1);
        long factorKKey = ThreadLocalRandom.current().nextLong();
        randomPropsEncryptAlgorithm = TypedSPILoader.getService(EncryptAlgorithm.class, "SphereEx:FASTOPE",
                PropertiesBuilder.build(new Property("alpha-key", alphaKey), new Property("factor-e-key", factorEKey), new Property("factor-k-key", factorKKey)));
        log.info("alphaKey: {}, factorEKey: {}, factorKKey: {}", alphaKey, factorEKey, factorKKey);
    }
    
    @SuppressWarnings("unchecked")
    @Test
    void assertEncryptOrder() {
        assertTrue(((Comparable<Object>) randomPropsEncryptAlgorithm.encrypt("200000", mock(AlgorithmSQLContext.class), mock(EncryptContext.class))).compareTo(
                randomPropsEncryptAlgorithm.encrypt("100000", mock(AlgorithmSQLContext.class), mock(EncryptContext.class))) > 0);
        assertTrue(((Comparable<Object>) randomPropsEncryptAlgorithm.encrypt("1", mock(AlgorithmSQLContext.class), mock(EncryptContext.class))).compareTo(
                randomPropsEncryptAlgorithm.encrypt("0", mock(AlgorithmSQLContext.class), mock(EncryptContext.class))) > 0);
        assertTrue(((Comparable<Object>) randomPropsEncryptAlgorithm.encrypt("20", mock(AlgorithmSQLContext.class), mock(EncryptContext.class))).compareTo(
                randomPropsEncryptAlgorithm.encrypt("100", mock(AlgorithmSQLContext.class), mock(EncryptContext.class))) > 0);
        assertTrue(((Comparable<Object>) randomPropsEncryptAlgorithm.encrypt("b", mock(AlgorithmSQLContext.class), mock(EncryptContext.class))).compareTo(
                randomPropsEncryptAlgorithm.encrypt("a", mock(AlgorithmSQLContext.class), mock(EncryptContext.class))) > 0);
    }
    
    @SuppressWarnings("unchecked")
    @Test
    void assertEncryptNumericOrder() {
        EncryptContext encryptContext = mock(EncryptContext.class);
        MySQLDatabaseType databaseType = new MySQLDatabaseType();
        when(encryptContext.getDatabaseType()).thenReturn(databaseType);
        when(encryptContext.getColumnDataType()).thenReturn(new EncryptColumnDataTypeContext("INT", "VARCHAR", "VARCHAR", "VARCHAR", "VARCHAR"));
        when(DataTypeRegistry.getDataType(databaseType.getType(), "INT")).thenReturn(Optional.of(Types.INTEGER));
        AlgorithmSQLContext algorithmSQLContext = mock(AlgorithmSQLContext.class);
        assertTrue(((Comparable<Object>) randomPropsEncryptAlgorithm.encrypt(200000, algorithmSQLContext, encryptContext)).compareTo(
                randomPropsEncryptAlgorithm.encrypt(100000, algorithmSQLContext, encryptContext)) > 0);
        assertTrue(((Comparable<Object>) randomPropsEncryptAlgorithm.encrypt(1, algorithmSQLContext, encryptContext)).compareTo(
                randomPropsEncryptAlgorithm.encrypt(0, algorithmSQLContext, encryptContext)) > 0);
        assertTrue(((Comparable<Object>) randomPropsEncryptAlgorithm.encrypt(100, algorithmSQLContext, encryptContext)).compareTo(
                randomPropsEncryptAlgorithm.encrypt(20, algorithmSQLContext, encryptContext)) > 0);
    }
    
    @SuppressWarnings("unchecked")
    @Test
    void assertEncryptDoubleOrder() {
        EncryptContext encryptContext = mock(EncryptContext.class);
        when(encryptContext.getDatabaseType()).thenReturn(databaseType);
        when(encryptContext.getColumnDataType()).thenReturn(new EncryptColumnDataTypeContext("DOUBLE", "VARCHAR", "VARCHAR", "VARCHAR", "VARCHAR"));
        when(DataTypeRegistry.getDataType(databaseType.getType(), "DOUBLE")).thenReturn(Optional.of(Types.DOUBLE));
        AlgorithmSQLContext algorithmSQLContext = mock(AlgorithmSQLContext.class);
        assertTrue(((Comparable<Object>) randomPropsEncryptAlgorithm.encrypt(200000, algorithmSQLContext, encryptContext)).compareTo(
                randomPropsEncryptAlgorithm.encrypt(100000, algorithmSQLContext, encryptContext)) > 0);
        assertTrue(((Comparable<Object>) randomPropsEncryptAlgorithm.encrypt(1, algorithmSQLContext, encryptContext)).compareTo(
                randomPropsEncryptAlgorithm.encrypt(0, algorithmSQLContext, encryptContext)) > 0);
        assertTrue(((Comparable<Object>) randomPropsEncryptAlgorithm.encrypt(100, algorithmSQLContext, encryptContext)).compareTo(
                randomPropsEncryptAlgorithm.encrypt(20, algorithmSQLContext, encryptContext)) > 0);
        assertTrue(((Comparable<Object>) randomPropsEncryptAlgorithm.encrypt(2.33, algorithmSQLContext, encryptContext)).compareTo(
                randomPropsEncryptAlgorithm.encrypt(-2.34, algorithmSQLContext, encryptContext)) > 0);
    }
    
    @SuppressWarnings("unchecked")
    @Test
    void assertEncryptDecimalOrder() {
        EncryptContext encryptContext = mock(EncryptContext.class);
        when(encryptContext.getDatabaseType()).thenReturn(databaseType);
        when(encryptContext.getColumnDataType()).thenReturn(new EncryptColumnDataTypeContext("DECIMAL(10,2)", "VARCHAR", "VARCHAR", "VARCHAR", "VARCHAR"));
        when(DataTypeRegistry.getDataType(databaseType.getType(), "DECIMAL")).thenReturn(Optional.of(Types.DECIMAL));
        AlgorithmSQLContext algorithmSQLContext = mock(AlgorithmSQLContext.class);
        Comparable<Object> encryptedValue0 = (Comparable<Object>) randomPropsEncryptAlgorithm.encrypt(-23, algorithmSQLContext, encryptContext);
        Comparable<Object> encryptedValue1 = (Comparable<Object>) randomPropsEncryptAlgorithm.encrypt(-0.01, algorithmSQLContext, encryptContext);
        Comparable<Object> encryptedValue2 = (Comparable<Object>) randomPropsEncryptAlgorithm.encrypt(0, algorithmSQLContext, encryptContext);
        Comparable<Object> encryptedValue3 = (Comparable<Object>) randomPropsEncryptAlgorithm.encrypt(0.01, algorithmSQLContext, encryptContext);
        Comparable<Object> encryptedValue4 = (Comparable<Object>) randomPropsEncryptAlgorithm.encrypt(2, algorithmSQLContext, encryptContext);
        Comparable<Object> encryptedValue5 = (Comparable<Object>) randomPropsEncryptAlgorithm.encrypt(10, algorithmSQLContext, encryptContext);
        Comparable<Object> encryptedValue6 = (Comparable<Object>) randomPropsEncryptAlgorithm.encrypt(99999999.99, algorithmSQLContext, encryptContext);
        List<Comparable<Object>> expected = Arrays.asList(encryptedValue0, encryptedValue1, encryptedValue2, encryptedValue3, encryptedValue4, encryptedValue5, encryptedValue6);
        List<Comparable<Object>> actual = Arrays.asList(encryptedValue0, encryptedValue1, encryptedValue2, encryptedValue3, encryptedValue4, encryptedValue5, encryptedValue6);
        actual.sort(Comparable::compareTo);
        assertIterableEquals(expected, actual);
    }
    
    @SuppressWarnings("unchecked")
    @Test
    void assertEncryptRandomStringOrder() {
        AlgorithmSQLContext algorithmSQLContext = mock(AlgorithmSQLContext.class);
        EncryptContext encryptContext = mock(EncryptContext.class);
        for (int i = 0; i < 10000; i++) {
            String randomStringA = RandomStringUtils.randomPrint(ThreadLocalRandom.current().nextInt(10));
            String randomStringB = RandomStringUtils.randomPrint(ThreadLocalRandom.current().nextInt(10));
            assertThat("randomStringA: " + randomStringA + ", randomStringB: " + randomStringB,
                    ((Comparable<Object>) randomPropsEncryptAlgorithm.encrypt(randomStringA, algorithmSQLContext, encryptContext)).compareTo(
                            randomPropsEncryptAlgorithm.encrypt(randomStringB, algorithmSQLContext, encryptContext)) > 0,
                    is(randomStringA.compareTo(randomStringB) > 0));
        }
    }
    
    @SuppressWarnings("unchecked")
    @Test
    void assertEncryptRandomNumericStringOrder() {
        AlgorithmSQLContext algorithmSQLContext = mock(AlgorithmSQLContext.class);
        EncryptContext encryptContext = mock(EncryptContext.class);
        for (int i = 0; i < 10000; i++) {
            String randomNumericA = RandomStringUtils.randomNumeric(ThreadLocalRandom.current().nextInt(10));
            String randomNumericB = RandomStringUtils.randomNumeric(ThreadLocalRandom.current().nextInt(10));
            assertThat("randomNumericA: " + randomNumericA + ", randomNumericB: " + randomNumericB,
                    ((Comparable<Object>) randomPropsEncryptAlgorithm.encrypt(randomNumericA, algorithmSQLContext, encryptContext)).compareTo(
                            randomPropsEncryptAlgorithm.encrypt(randomNumericB, algorithmSQLContext, encryptContext)) > 0,
                    is(randomNumericA.compareTo(randomNumericB) > 0));
        }
    }
    
    @SuppressWarnings("unchecked")
    @Test
    void assertEncryptRandomNumericOrder() {
        AlgorithmSQLContext algorithmSQLContext = mock(AlgorithmSQLContext.class);
        EncryptContext encryptContext = mock(EncryptContext.class);
        MySQLDatabaseType databaseType = new MySQLDatabaseType();
        when(encryptContext.getDatabaseType()).thenReturn(databaseType);
        when(encryptContext.getColumnDataType()).thenReturn(new EncryptColumnDataTypeContext("INT", "VARCHAR", "VARCHAR", "VARCHAR", "VARCHAR"));
        when(DataTypeRegistry.getDataType(databaseType.getType(), "INT")).thenReturn(Optional.of(Types.INTEGER));
        for (int i = 0; i < 10000; i++) {
            Integer randomNumericA = ThreadLocalRandom.current().nextInt();
            Integer randomNumericB = ThreadLocalRandom.current().nextInt();
            assertThat("randomNumericA: " + randomNumericA + ", randomNumericB: " + randomNumericB,
                    ((Comparable<Object>) randomPropsEncryptAlgorithm.encrypt(randomNumericA, algorithmSQLContext, encryptContext)).compareTo(
                            randomPropsEncryptAlgorithm.encrypt(randomNumericB, algorithmSQLContext, encryptContext)) > 0,
                    is(randomNumericA.compareTo(randomNumericB) > 0));
        }
    }
    
    @Test
    void assertEncryptNullValue() {
        assertNull(encryptAlgorithm.encrypt(null, mock(AlgorithmSQLContext.class), mock(EncryptContext.class)));
    }
    
    @Test
    void assertEncryptWithMetaData() {
        EncryptContext encryptContext = mock(EncryptContext.class);
        when(encryptContext.getColumnDataType()).thenReturn(new EncryptColumnDataTypeContext("INT", "VARCHAR", "VARCHAR", "VARCHAR", "VARCHAR"));
        OracleDatabaseType databaseType = new OracleDatabaseType();
        when(encryptContext.getDatabaseType()).thenReturn(databaseType);
        when(DataTypeRegistry.getDataType(databaseType.getType(), "INT")).thenReturn(Optional.of(Types.INTEGER));
        assertThat(encryptAlgorithm.encrypt(3, mock(AlgorithmSQLContext.class), encryptContext), is("A7KHHHGOOOCII00="));
    }
    
    @Test
    void assertDecrypt() {
        assertThat(encryptAlgorithm.decrypt("7KJ3IG9SSGUIC00=", mock(AlgorithmSQLContext.class), mock(EncryptContext.class)).toString(), is("test"));
    }
    
    @Test
    void assertDecryptRandom() {
        AlgorithmSQLContext algorithmSQLContext = mock(AlgorithmSQLContext.class);
        EncryptContext encryptContext = mock(EncryptContext.class);
        for (int i = 0; i < 10000; i++) {
            String randomString = RandomStringUtils.randomPrint(ThreadLocalRandom.current().nextInt(100));
            assertThat("randomString: " + randomString,
                    randomPropsEncryptAlgorithm.decrypt(randomPropsEncryptAlgorithm.encrypt(randomString, algorithmSQLContext, encryptContext), algorithmSQLContext, encryptContext).toString(),
                    is(randomString));
        }
    }
    
    @Test
    void assertDecryptNullValue() {
        assertNull(encryptAlgorithm.decrypt(null, mock(AlgorithmSQLContext.class), mock(EncryptContext.class)));
    }
    
    @Test
    void assertDecryptWithMetaData() {
        EncryptContext encryptContext = mock(EncryptContext.class);
        when(encryptContext.getColumnDataType()).thenReturn(new EncryptColumnDataTypeContext("INT", "VARCHAR", "VARCHAR", "VARCHAR", "VARCHAR"));
        OracleDatabaseType databaseType = new OracleDatabaseType();
        when(encryptContext.getDatabaseType()).thenReturn(databaseType);
        when(DataTypeRegistry.getDataType(databaseType.getType(), "INT")).thenReturn(Optional.of(Types.INTEGER));
        assertThat(encryptAlgorithm.decrypt("A7KHHHGOOOCII00=", mock(AlgorithmSQLContext.class), encryptContext), is(3));
    }
}
