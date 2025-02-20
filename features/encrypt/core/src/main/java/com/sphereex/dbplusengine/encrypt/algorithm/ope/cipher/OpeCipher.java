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

import com.sphereex.dbplusengine.encrypt.algorithm.ope.util.OpeEncoder;

import java.nio.charset.Charset;
import java.util.Optional;
import java.util.function.Function;

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
     * @param charset charset
     * @return supported function
     */
    default Optional<Function<Object, byte[]>> getSupportedEncryptFunction(final Class<?> clazz, final Charset charset) {
        if (clazz == boolean.class || clazz == Boolean.class) {
            return Optional.of(optional -> encryptBoolean((boolean) optional));
        }
        if (clazz == byte.class || clazz == Byte.class) {
            return Optional.of(optional -> encryptByte((byte) optional));
        }
        if (clazz == short.class || clazz == Short.class) {
            return Optional.of(optional -> encryptShort((short) optional));
        }
        if (clazz == int.class || clazz == Integer.class) {
            return Optional.of(optional -> encryptInt((int) optional));
        }
        if (clazz == long.class || clazz == Long.class) {
            return Optional.of(optional -> encryptLong((long) optional));
        }
        if (clazz == float.class || clazz == Float.class) {
            return Optional.of(optional -> encryptFloat((float) optional));
        }
        if (clazz == double.class || clazz == Double.class) {
            return Optional.of(optional -> encryptDouble((double) optional));
        }
        if (clazz == char.class || clazz == Character.class) {
            return Optional.of(optional -> encryptChar((char) optional));
        }
        if (clazz == String.class) {
            return Optional.of(optional -> encryptString((String) optional, charset));
        }
        return Optional.empty();
    }
    
    /**
     * Get supported decrypt function.
     *
     * @param clazz class
     * @param charset charset
     * @return supported function
     */
    default Optional<Function<byte[], Object>> getSupportedDecryptFunction(final Class<?> clazz, final Charset charset) {
        if (clazz == boolean.class || clazz == Boolean.class) {
            return Optional.of(this::decryptBoolean);
        }
        if (clazz == byte.class || clazz == Byte.class) {
            return Optional.of(this::decryptByte);
        }
        if (clazz == short.class || clazz == Short.class) {
            return Optional.of(this::decryptShort);
        }
        if (clazz == int.class || clazz == Integer.class) {
            return Optional.of(this::decryptInt);
        }
        if (clazz == long.class || clazz == Long.class) {
            return Optional.of(this::decryptLong);
        }
        if (clazz == float.class || clazz == Float.class) {
            return Optional.of(this::decryptFloat);
        }
        if (clazz == double.class || clazz == Double.class) {
            return Optional.of(this::decryptDouble);
        }
        if (clazz == char.class || clazz == Character.class) {
            return Optional.of(this::decryptChar);
        }
        if (clazz == String.class) {
            return Optional.of(optional -> decryptString(optional, charset));
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
