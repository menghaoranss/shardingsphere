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

import com.sphereex.dbplusengine.encrypt.exception.algorithm.OpeAlgorithmException;
import lombok.Getter;

import java.math.BigInteger;

/**
 * Mope cipher.
 */
public final class MopeCipher implements OpeCipher {
    
    @Getter
    private final OpeCipher cipher;
    
    private final int plaintextBytes;
    
    private final BigInteger offset;
    
    private final BigInteger max;
    
    public MopeCipher(final OpeCipher cipher, final int plaintextBytes, final BigInteger offset) {
        this.cipher = cipher;
        this.plaintextBytes = plaintextBytes;
        this.offset = offset;
        BigInteger max = BigInteger.valueOf(1L);
        for (int i = 0; i < plaintextBytes; i++) {
            max = max.shiftLeft(8);
        }
        this.max = max;
    }
    
    /**
     * Encrypt.
     *
     * @param plaintext plaintext
     * @return ciphertext
     * @throws OpeAlgorithmException OpeAlgorithmException
     */
    public byte[] encrypt(final byte[] plaintext) {
        if (plaintext.length > plaintextBytes) {
            throw new OpeAlgorithmException("Plaintext cannot exceed " + plaintextBytes + " bytes in size.");
        }
        byte[] temp = new byte[plaintext.length + 1];
        System.arraycopy(plaintext, 0, temp, 1, plaintext.length);
        BigInteger plain = new BigInteger(temp);
        BigInteger subtractPlain = plain.subtract(offset);
        if (subtractPlain.compareTo(BigInteger.valueOf(0L)) < 0) {
            subtractPlain = subtractPlain.add(max);
        }
        subtractPlain = subtractPlain.mod(max);
        byte[] bytes;
        temp = subtractPlain.toByteArray();
        if (temp.length == plaintextBytes) {
            bytes = temp;
        } else {
            bytes = new byte[plaintextBytes];
            if (temp.length < plaintextBytes) {
                System.arraycopy(temp, 0, bytes, plaintextBytes - temp.length, temp.length);
            } else {
                System.arraycopy(temp, temp.length - plaintextBytes, bytes, 0, plaintextBytes);
            }
        }
        return cipher.encrypt(bytes);
    }
    
    /**
     * Decrypt.
     *
     * @param ciphertext ciphertext
     * @return plaintext
     * @throws OpeAlgorithmException OpeAlgorithmException
     */
    public byte[] decrypt(final byte[] ciphertext) {
        byte[] bytes = cipher.decrypt(ciphertext);
        if (bytes.length > plaintextBytes) {
            throw new OpeAlgorithmException("Plaintext cannot exceed " + plaintextBytes + " bytes in size.");
        }
        byte[] temp = new byte[bytes.length + 1];
        System.arraycopy(bytes, 0, temp, 1, bytes.length);
        BigInteger plain2 = new BigInteger(temp);
        BigInteger plain = plain2.add(offset).mod(max);
        byte[] result;
        temp = plain.toByteArray();
        if (temp.length == plaintextBytes) {
            result = temp;
        } else {
            result = new byte[plaintextBytes];
            if (temp.length < plaintextBytes) {
                System.arraycopy(temp, 0, result, plaintextBytes - temp.length, temp.length);
            } else {
                System.arraycopy(temp, temp.length - plaintextBytes, result, 0, plaintextBytes);
            }
        }
        return result;
    }
}
