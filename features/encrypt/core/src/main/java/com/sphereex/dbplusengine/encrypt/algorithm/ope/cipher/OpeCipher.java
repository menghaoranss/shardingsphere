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

package com.sphereex.dbplusengine.encrypt.algorithm.ope.cipher;

import com.sphereex.dbplusengine.encrypt.algorithm.ope.context.OpeContext;
import com.sphereex.dbplusengine.encrypt.algorithm.ope.util.OpeEncoder;
import com.sphereex.dbplusengine.encrypt.exception.algorithm.OpeAlgorithmException;

import java.math.BigDecimal;
import java.nio.charset.Charset;
import java.util.Optional;
import java.util.function.BiFunction;

/**
 * Ope cipher.
 */
public interface OpeCipher {
    
    /**
     * Encrypt.
     *
     * @param plaintext plaintext
     * @return ciphertext
     */
    byte[] encrypt(byte[] plaintext);
    
    /**
     * Get supported encrypt function.
     *
     * @param clazz class
     * @return supported function
     * @throws OpeAlgorithmException if precision and scale are required for decimal encryption
     */
    default Optional<BiFunction<Object, OpeContext, byte[]>> getSupportedEncryptFunction(final Class<?> clazz) {
        if (clazz == boolean.class || clazz == Boolean.class) {
            return Optional.of((value, opeContext) -> encryptBoolean((boolean) value));
        }
        if (clazz == byte.class || clazz == Byte.class) {
            return Optional.of((value, opeContext) -> encryptByte((byte) value));
        }
        if (clazz == short.class || clazz == Short.class) {
            return Optional.of((value, opeContext) -> encryptShort((short) value));
        }
        if (clazz == int.class || clazz == Integer.class) {
            return Optional.of((value, opeContext) -> encryptInt((int) value));
        }
        if (clazz == long.class || clazz == Long.class) {
            return Optional.of((value, opeContext) -> encryptLong((long) value));
        }
        if (clazz == float.class || clazz == Float.class) {
            return Optional.of((value, opeContext) -> encryptFloat((float) value));
        }
        if (clazz == double.class || clazz == Double.class) {
            return Optional.of((value, opeContext) -> encryptDouble((double) value));
        }
        if (clazz == char.class || clazz == Character.class) {
            return Optional.of((value, opeContext) -> encryptChar((char) value));
        }
        if (clazz == String.class) {
            return Optional.of((value, opeContext) -> encryptString((String) value, opeContext.getCharset()));
        }
        if (clazz == BigDecimal.class) {
            return Optional.of((value, opeContext) -> encrypt(OpeEncoder.encodeBigDecimal((BigDecimal) value, opeContext.getPrecision(), opeContext.getScale(), opeContext.getRoundingMode())));
        }
        return Optional.empty();
    }
    
    /**
     * Get supported decrypt function.
     *
     * @param clazz class
     * @return supported function
     */
    default Optional<BiFunction<byte[], OpeContext, Object>> getSupportedDecryptFunction(final Class<?> clazz) {
        if (clazz == boolean.class || clazz == Boolean.class) {
            return Optional.of((value, opeContext) -> decryptBoolean(value));
        }
        if (clazz == byte.class || clazz == Byte.class) {
            return Optional.of((value, opeContext) -> decryptByte(value));
        }
        if (clazz == short.class || clazz == Short.class) {
            return Optional.of((value, opeContext) -> decryptShort(value));
        }
        if (clazz == int.class || clazz == Integer.class) {
            return Optional.of((value, opeContext) -> decryptInt(value));
        }
        if (clazz == long.class || clazz == Long.class) {
            return Optional.of((value, opeContext) -> decryptLong(value));
        }
        if (clazz == float.class || clazz == Float.class) {
            return Optional.of((value, opeContext) -> decryptFloat(value));
        }
        if (clazz == double.class || clazz == Double.class) {
            return Optional.of((value, opeContext) -> decryptDouble(value));
        }
        if (clazz == char.class || clazz == Character.class) {
            return Optional.of((value, opeContext) -> decryptChar(value));
        }
        if (clazz == String.class) {
            return Optional.of((value, opeContext) -> decryptString(value, opeContext.getCharset()));
        }
        if (clazz == BigDecimal.class) {
            return Optional.of((value, opeContext) -> OpeEncoder.decodeBigDecimal(decrypt(value), opeContext.getPrecision(), opeContext.getScale()));
        }
        return Optional.empty();
    }
    
    /**
     * encrypt boolean.
     *
     * @param plaintext plaintext
     * @return ciphertext
     */
    default byte[] encryptBoolean(final boolean plaintext) {
        return encrypt(OpeEncoder.encodeBoolean(plaintext));
    }
    
    /**
     * Encrypt byte.
     *
     * @param plaintext plaintext
     * @return ciphertext
     */
    default byte[] encryptByte(final byte plaintext) {
        return encrypt(OpeEncoder.encodeByte(plaintext));
    }
    
    /**
     * Encrypt short.
     *
     * @param plaintext plaintext
     * @return ciphertext
     */
    default byte[] encryptShort(final short plaintext) {
        return encrypt(OpeEncoder.encodeShort(plaintext));
    }
    
    /**
     * Encrypt int.
     *
     * @param plaintext plaintext
     * @return ciphertext
     */
    default byte[] encryptInt(final int plaintext) {
        return encrypt(OpeEncoder.encodeInt(plaintext));
    }
    
    /**
     * Encrypt long.
     *
     * @param plaintext plaintext
     * @return ciphertext
     */
    default byte[] encryptLong(final long plaintext) {
        return encrypt(OpeEncoder.encodeLong(plaintext));
    }
    
    /**
     * Encrypt float.
     *
     * @param plaintext plaintext
     * @return ciphertext
     */
    default byte[] encryptFloat(final float plaintext) {
        return encrypt(OpeEncoder.encodeFloat(plaintext));
    }
    
    /**
     * Encrypt double.
     *
     * @param plaintext plaintext
     * @return ciphertext
     */
    default byte[] encryptDouble(final double plaintext) {
        return encrypt(OpeEncoder.encodeDouble(plaintext));
    }
    
    /**
     * Encrypt char.
     *
     * @param plaintext plaintext
     * @return ciphertext
     */
    default byte[] encryptChar(final char plaintext) {
        return encrypt(OpeEncoder.encodeChar(plaintext));
    }
    
    /**
     * Encrypt string.
     *
     * @param plaintext plaintext
     * @param charset charset
     * @return ciphertext
     */
    default byte[] encryptString(final String plaintext, final Charset charset) {
        return encrypt(OpeEncoder.encodeString(plaintext, charset));
    }
    
    /**
     * Decode key.
     *
     * @param ciphertext ciphertext
     * @return plaintext
     */
    byte[] decrypt(byte[] ciphertext);
    
    /**
     * Decrypt boolean.
     *
     * @param ciphertext ciphertext
     * @return plaintext
     */
    default boolean decryptBoolean(final byte[] ciphertext) {
        return OpeEncoder.decodeBoolean(decrypt(ciphertext));
    }
    
    /**
     * Decrypt byte.
     *
     * @param ciphertext ciphertext
     * @return plaintext
     */
    default byte decryptByte(final byte[] ciphertext) {
        return OpeEncoder.decodeByte(decrypt(ciphertext));
    }
    
    /**
     * Decrypt short.
     *
     * @param ciphertext ciphertext
     * @return plaintext
     */
    default short decryptShort(final byte[] ciphertext) {
        return OpeEncoder.decodeShort(decrypt(ciphertext));
    }
    
    /**
     * Decrypt int.
     *
     * @param ciphertext ciphertext
     * @return plaintext
     */
    default int decryptInt(final byte[] ciphertext) {
        return OpeEncoder.decodeInt(decrypt(ciphertext));
    }
    
    /**
     * Decrypt long.
     *
     * @param ciphertext ciphertext
     * @return plaintext
     */
    default long decryptLong(final byte[] ciphertext) {
        return OpeEncoder.decodeLong(decrypt(ciphertext));
    }
    
    /**
     * Decrypt float.
     *
     * @param ciphertext ciphertext
     * @return plaintext
     */
    default float decryptFloat(final byte[] ciphertext) {
        return OpeEncoder.decodeFloat(decrypt(ciphertext));
    }
    
    /**
     * Decrypt double.
     *
     * @param ciphertext ciphertext
     * @return plaintext
     */
    default double decryptDouble(final byte[] ciphertext) {
        return OpeEncoder.decodeDouble(decrypt(ciphertext));
    }
    
    /**
     * Decrypt char.
     *
     * @param ciphertext ciphertext
     * @return plaintext
     */
    default char decryptChar(final byte[] ciphertext) {
        return OpeEncoder.decodeChar(decrypt(ciphertext));
    }
    
    /**
     * Decrypt string.
     *
     * @param ciphertext ciphertext
     * @param charset charset
     * @return plaintext
     */
    default String decryptString(final byte[] ciphertext, final Charset charset) {
        return OpeEncoder.decodeString(decrypt(ciphertext), charset);
    }
}
