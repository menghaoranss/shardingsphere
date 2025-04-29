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
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.apache.shardingsphere.infra.exception.core.ShardingSpherePreconditions;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * OPE encoder.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class OpeEncoder {
    
    private static final Map<Integer, BigInteger> OFFSET_VALUE_CACHE = new ConcurrentHashMap<>();
    
    /**
     * Encode boolean.
     *
     * @param value value
     * @return encoded value
     */
    public static byte[] encodeBoolean(final boolean value) {
        return new byte[]{(byte) (value ? 1 : 0)};
    }
    
    /**
     * Decode boolean.
     *
     * @param value value
     * @return decoded value
     */
    public static boolean decodeBoolean(final byte[] value) {
        checkLength(value, 1);
        return 0 != value[0];
    }
    
    /**
     * Encode byte.
     *
     * @param value value
     * @return encoded value
     */
    public static byte[] encodeByte(final byte value) {
        BigInteger v = BigInteger.valueOf(value).subtract(BigInteger.valueOf(Byte.MIN_VALUE));
        return new byte[]{v.byteValue()};
    }
    
    /**
     * Decode byte.
     *
     * @param value value
     * @return decoded value
     */
    public static byte decodeByte(final byte[] value) {
        checkLength(value, 1);
        return new BigInteger(value).add(BigInteger.valueOf(Byte.MIN_VALUE)).byteValue();
    }
    
    /**
     * Encode short.
     *
     * @param value value
     * @return encoded value
     */
    public static byte[] encodeShort(final short value) {
        BigInteger v = BigInteger.valueOf(value).subtract(BigInteger.valueOf(Short.MIN_VALUE));
        return new byte[]{
                v.shiftRight(8).byteValue(),
                v.byteValue()
        };
    }
    
    /**
     * Decode short.
     *
     * @param value value
     * @return decoded value
     */
    public static short decodeShort(final byte[] value) {
        checkLength(value, 2);
        return new BigInteger(value).add(BigInteger.valueOf(Short.MIN_VALUE)).shortValue();
    }
    
    /**
     * Encode int.
     *
     * @param value value
     * @return encoded value
     */
    public static byte[] encodeInt(final int value) {
        BigInteger v = BigInteger.valueOf(value).subtract(BigInteger.valueOf(Integer.MIN_VALUE));
        return new byte[]{
                v.shiftRight(24).byteValue(),
                v.shiftRight(16).byteValue(),
                v.shiftRight(8).byteValue(),
                v.byteValue()
        };
    }
    
    /**
     * Decode int.
     *
     * @param value value
     * @return decoded value
     */
    public static int decodeInt(final byte[] value) {
        checkLength(value, 4);
        return new BigInteger(value).add(BigInteger.valueOf(Integer.MIN_VALUE)).intValue();
    }
    
    /**
     * Encode long.
     *
     * @param value value
     * @return encoded value
     */
    public static byte[] encodeLong(final long value) {
        BigInteger v = BigInteger.valueOf(value).subtract(BigInteger.valueOf(Long.MIN_VALUE));
        return new byte[]{
                v.shiftRight(56).byteValue(),
                v.shiftRight(48).byteValue(),
                v.shiftRight(40).byteValue(),
                v.shiftRight(32).byteValue(),
                v.shiftRight(24).byteValue(),
                v.shiftRight(16).byteValue(),
                v.shiftRight(8).byteValue(),
                v.byteValue()
        };
    }
    
    /**
     * Decode long.
     *
     * @param value value
     * @return decoded value
     */
    public static long decodeLong(final byte[] value) {
        checkLength(value, 8);
        return new BigInteger(value).add(BigInteger.valueOf(Long.MIN_VALUE)).longValue();
    }
    
    /**
     * Encode float.
     *
     * @param value value
     * @return encoded value
     */
    public static byte[] encodeFloat(final float value) {
        ByteBuffer byteBuffer = ByteBuffer.allocate(4);
        byteBuffer.putFloat(value);
        byte[] result = byteBuffer.array();
        if ((result[0] & 0x80) == 0) {
            result[0] |= (byte) 0x80;
        } else {
            for (int i = 0; i < result.length; i++) {
                result[i] = (byte) ~result[i];
            }
        }
        return result;
    }
    
    /**
     * Decode float.
     *
     * @param value value
     * @return decoded value
     */
    public static float decodeFloat(final byte[] value) {
        checkLength(value, 4);
        ByteBuffer result = ByteBuffer.allocate(4);
        result.put(value);
        result.position(0);
        if ((value[0] & 0x80) == 0) {
            for (byte each : value) {
                result.put((byte) ~each);
            }
        } else {
            result.put((byte) (value[0] & 0x7f));
        }
        result.position(0);
        return result.getFloat();
    }
    
    /**
     * Encode double.
     *
     * @param value value
     * @return encoded value
     */
    public static byte[] encodeDouble(final double value) {
        ByteBuffer byteBuffer = ByteBuffer.allocate(8);
        byteBuffer.putDouble(value);
        byte[] result = byteBuffer.array();
        if ((result[0] & 0x80) == 0) {
            result[0] |= (byte) 0x80;
        } else {
            for (int i = 0; i < result.length; i++) {
                result[i] = (byte) ~result[i];
            }
        }
        return result;
    }
    
    /**
     * Decode double.
     *
     * @param value value
     * @return decoded value
     */
    public static double decodeDouble(final byte[] value) {
        checkLength(value, 8);
        ByteBuffer result = ByteBuffer.allocate(8);
        result.put(value);
        result.position(0);
        if ((value[0] & 0x80) == 0) {
            for (byte each : value) {
                result.put((byte) ~each);
            }
        } else {
            result.put((byte) (value[0] & 0x7f));
        }
        result.position(0);
        return result.getDouble();
    }
    
    /**
     * Encode char.
     *
     * @param value value
     * @return encoded value
     */
    public static byte[] encodeChar(final char value) {
        return new byte[]{
                (byte) (value >> 8),
                (byte) (value & 0xff)
        };
    }
    
    /**
     * Decode char.
     *
     * @param value value
     * @return decoded value
     */
    public static char decodeChar(final byte[] value) {
        checkLength(value, 2);
        return (char) ((value[0] << 8) | value[1]);
    }
    
    /**
     * Encode string.
     *
     * @param value value
     * @param charset charset
     * @return encoded value
     */
    public static byte[] encodeString(final String value, final Charset charset) {
        if (null == value) {
            return null;
        } else {
            return value.getBytes(charset);
        }
    }
    
    /**
     * Decode string.
     *
     * @param value value
     * @param charset charset
     * @return decoded value
     */
    public static String decodeString(final byte[] value, final Charset charset) {
        if (null == value) {
            return null;
        } else {
            return new String(value, charset);
        }
    }
    
    /**
     * Encode BigDecimal.
     *
     * @param value value
     * @param precision precision
     * @param scale scale
     * @param roundingMode rounding mode
     * @return encoded value
     * @throws OpeAlgorithmException if precision and scale are required for decimal encryption
     * @throws OpeAlgorithmException if value is too large
     */
    public static byte[] encodeBigDecimal(final BigDecimal value, final Integer precision, final Integer scale, final RoundingMode roundingMode) {
        if (null == value) {
            return null;
        }
        if (null == precision || null == scale) {
            throw new OpeAlgorithmException("Precision and scale are required for decimal encryption");
        }
        BigDecimal rescaledValue = value.setScale(scale, roundingMode);
        if (rescaledValue.precision() > precision) {
            throw new OpeAlgorithmException("Value is too large");
        }
        int bitLength = getBitLength(precision);
        byte[] valueByteArray = rescaledValue.unscaledValue().add(getOffsetValue(bitLength)).toByteArray();
        byte[] result = new byte[(bitLength + 7) >> 3];
        if (0 == valueByteArray[0]) {
            System.arraycopy(valueByteArray, 1, result, result.length - valueByteArray.length + 1, valueByteArray.length - 1);
        } else {
            System.arraycopy(valueByteArray, 0, result, result.length - valueByteArray.length, valueByteArray.length);
        }
        return result;
    }
    
    /**
     * Decode BigDecimal.
     *
     * @param value value
     * @param precision precision
     * @param scale scale
     * @return decoded value
     * @throws OpeAlgorithmException if precision and scale are required for decimal encryption
     */
    public static BigDecimal decodeBigDecimal(final byte[] value, final Integer precision, final Integer scale) {
        if (null == precision || null == scale) {
            throw new OpeAlgorithmException("Precision and scale are required for decimal encryption");
        }
        int bitLength = getBitLength(precision);
        int byteLength = (bitLength + 7) >> 3;
        checkLength(value, byteLength);
        byte[] valueByteArray = value;
        if (0 != (value[0] & 0x80)) {
            byte[] temp = new byte[byteLength + 1];
            System.arraycopy(value, 0, temp, 1, value.length);
            valueByteArray = temp;
        }
        BigInteger bigInteger = new BigInteger(valueByteArray).subtract(getOffsetValue(bitLength));
        return new BigDecimal(bigInteger, scale);
    }
    
    private static BigInteger getOffsetValue(final int bitLength) {
        BigInteger result;
        if (null == (result = OFFSET_VALUE_CACHE.get(bitLength))) {
            result = OFFSET_VALUE_CACHE.computeIfAbsent(bitLength, key -> BigInteger.ONE.shiftLeft(bitLength - 1));
        }
        return result;
    }
    
    private static int getBitLength(final Integer precision) {
        // NOTE : 运算转为常量提升性能
        // 3.3219280948873626 = Math.log(10) / Math.log(2)
        return (int) (precision * 3.3219280948873626 + 2);
    }
    
    private static void checkLength(final byte[] value, final int expectedLength) {
        ShardingSpherePreconditions.checkNotNull(value, () -> new OpeAlgorithmException("Input value is null."));
        ShardingSpherePreconditions.checkState(value.length == expectedLength,
                () -> new OpeAlgorithmException("Invalid byte array length. Expecting " + expectedLength + ", found " + value.length + "."));
    }
}
