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

package com.sphereex.dbplusengine.encrypt.algorithm.ope.util;

import com.sphereex.dbplusengine.encrypt.exception.algorithm.OpeAlgorithmException;
import org.apache.commons.codec.binary.Base32;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class OpeEncoderTest {
    
    private static final Base32 BASE32 = new Base32(true);
    
    @Test
    void assertEncodeBigDecimalOrder() {
        String num0 = BASE32.encodeAsString(OpeEncoder.encodeBigDecimal(new BigDecimal("-11125.25"), 10, 2, RoundingMode.HALF_UP));
        String num1 = BASE32.encodeAsString(OpeEncoder.encodeBigDecimal(new BigDecimal("-23"), 10, 2, RoundingMode.HALF_UP));
        String num2 = BASE32.encodeAsString(OpeEncoder.encodeBigDecimal(new BigDecimal("-0.01"), 10, 2, RoundingMode.HALF_UP));
        String num3 = BASE32.encodeAsString(OpeEncoder.encodeBigDecimal(new BigDecimal("2.33"), 10, 2, RoundingMode.HALF_UP));
        String num4 = BASE32.encodeAsString(OpeEncoder.encodeBigDecimal(new BigDecimal("5"), 10, 2, RoundingMode.HALF_UP));
        String num5 = BASE32.encodeAsString(OpeEncoder.encodeBigDecimal(new BigDecimal("20.8"), 10, 2, RoundingMode.HALF_UP));
        String num6 = BASE32.encodeAsString(OpeEncoder.encodeBigDecimal(new BigDecimal("99999999"), 10, 2, RoundingMode.HALF_UP));
        List<String> expected = Arrays.asList(num0, num1, num2, num3, num4, num5, num6);
        List<String> actual = Arrays.asList(num0, num1, num2, num3, num4, num5, num6);
        actual.sort(String::compareTo);
        assertIterableEquals(expected, actual);
    }
    
    @Test
    void assertEncodeBigDecimalLargeVlaue() {
        assertThrows(OpeAlgorithmException.class, () -> OpeEncoder.encodeBigDecimal(new BigDecimal("-11125.25"), 5, 2, RoundingMode.HALF_UP));
        assertThrows(OpeAlgorithmException.class, () -> OpeEncoder.encodeBigDecimal(new BigDecimal("999"), 3, 1, RoundingMode.HALF_UP));
    }
    
    @Test
    void assertEncodeBigDecimalRound() {
        String num0 = BASE32.encodeAsString(OpeEncoder.encodeBigDecimal(new BigDecimal("-11125.254"), 10, 2, RoundingMode.HALF_UP));
        String num1 = BASE32.encodeAsString(OpeEncoder.encodeBigDecimal(new BigDecimal("-11125.245"), 10, 2, RoundingMode.HALF_UP));
        assertEquals(num0, num1);
    }
    
    @Test
    void assertEncodeDouble() {
        String num0 = BASE32.encodeAsString(OpeEncoder.encodeDouble(new BigDecimal("-11125.25").doubleValue()));
        String num1 = BASE32.encodeAsString(OpeEncoder.encodeDouble(new BigDecimal("-2.34").doubleValue()));
        String num2 = BASE32.encodeAsString(OpeEncoder.encodeDouble(new BigDecimal("2.33").doubleValue()));
        String num3 = BASE32.encodeAsString(OpeEncoder.encodeDouble(new BigDecimal("5").doubleValue()));
        String num4 = BASE32.encodeAsString(OpeEncoder.encodeDouble(new BigDecimal("20.8").doubleValue()));
        String num5 = BASE32.encodeAsString(OpeEncoder.encodeDouble(new BigDecimal("99999999").doubleValue()));
        List<String> expected = Arrays.asList(num0, num1, num2, num3, num4, num5);
        List<String> actual = Arrays.asList(num0, num1, num2, num3, num4, num5);
        actual.sort(String::compareTo);
        assertIterableEquals(expected, actual);
    }
    
    @Test
    void assertDecodeBigDecimal() {
        BigDecimal expected = new BigDecimal("-11125.25");
        assertEquals(expected, OpeEncoder.decodeBigDecimal(OpeEncoder.encodeBigDecimal(expected, 10, 2, RoundingMode.HALF_UP), 10, 2));
        assertEquals(expected.doubleValue(), OpeEncoder.decodeBigDecimal(OpeEncoder.encodeBigDecimal(expected, 128, 10, RoundingMode.HALF_UP), 128, 10).doubleValue());
        expected = new BigDecimal("9999999999");
        assertEquals(expected, OpeEncoder.decodeBigDecimal(OpeEncoder.encodeBigDecimal(expected, 38, 0, RoundingMode.HALF_UP), 38, 0));
    }
    
    @Test
    void assertDecodeDouble() {
        double expected = new BigDecimal("-11125.25").doubleValue();
        assertEquals(expected, OpeEncoder.decodeDouble(OpeEncoder.encodeDouble(expected)));
    }
}
