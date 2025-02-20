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

import org.apache.commons.codec.digest.DigestUtils;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.security.MessageDigest;

/**
 * Fast ope cipher.
 */
public final class FastOpeCipher implements OpeCipher {
    
    private final long[] transformArray;
    
    private final byte[] kBytes;
    
    private final int plaintextBytesPerBlock;
    
    private final int ciphertextBytesPerBlock;
    
    private final int ciphertextBitsPerByte;
    
    private final long[] fMin;
    
    private final long[] fRange;
    
    private final int[] bitMasks = new int[]{
            0x00,
            0x80,
            0x40,
            0x20,
            0x10,
            0x08,
            0x04,
            0x02,
            0x01
    };
    
    public FastOpeCipher(final long factorN, final double alpha, final double factorE, final long factorK) {
        ByteBuffer kBuffer = ByteBuffer.allocate(Long.BYTES);
        kBuffer.putLong(factorK);
        this.kBytes = kBuffer.array();
        this.fMin = new long[9];
        long[] fMax = new long[9];
        this.fRange = new long[9];
        BigDecimal bigAlpha = new BigDecimal(alpha);
        double beta = 1.0 - alpha;
        BigDecimal bigBeta = new BigDecimal(beta);
        BigDecimal bigN = new BigDecimal(factorN);
        BigDecimal bigE = new BigDecimal(factorE);
        for (int i = 0; i < 9; i++) {
            BigDecimal factor = bigN.multiply(bigE.pow(i));
            fMin[i] = bigAlpha.multiply(factor).setScale(0, RoundingMode.FLOOR).longValue();
            fMax[i] = bigBeta.multiply(factor).setScale(0, RoundingMode.CEILING).longValue();
            this.fRange[i] = fMax[i] - fMin[i];
        }
        int cipherBits = 0;
        for (long b = factorN; b > 0L; b >>= 1L) {
            cipherBits++;
        }
        ciphertextBitsPerByte = cipherBits;
        int plaintextBytesPerBlock = 0;
        for (int i = 1; i <= 8; i++) {
            if ((cipherBits * i) % 8 == 0) {
                plaintextBytesPerBlock = i;
                break;
            }
        }
        this.plaintextBytesPerBlock = plaintextBytesPerBlock;
        ciphertextBytesPerBlock = plaintextBytesPerBlock * cipherBits / 8;
        this.transformArray = getTransformArray();
    }
    
    /**
     * Encrypt.
     *
     * @param plaintext plaintext
     * @return ciphertext
     */
    public byte[] encrypt(final byte[] plaintext) {
        int blockCount = (plaintext.length + plaintextBytesPerBlock - 1) / plaintextBytesPerBlock;
        int ciphertextSize = blockCount * ciphertextBytesPerBlock + 1;
        int padding = blockCount * plaintextBytesPerBlock - plaintext.length;
        ByteBuffer plaintextBuffer = ByteBuffer.wrap(plaintext);
        ByteBuffer result = ByteBuffer.allocate(ciphertextSize);
        for (int block = 0; block < blockCount; block++) {
            int totalBits = plaintextBytesPerBlock * ciphertextBitsPerByte;
            int totalBytes = (totalBits + 7) / 8;
            byte[] blockCipherBytes = new byte[totalBytes];
            int bitIndex = totalBits;
            for (int i = 0; i < plaintextBytesPerBlock; i++) {
                long cipher = 0L;
                if (plaintextBuffer.position() < plaintext.length) {
                    int bByte = Byte.toUnsignedInt(plaintextBuffer.get());
                    cipher = transform(0, 0);
                    for (int j = 1; j <= 8; j++) {
                        cipher += ((bByte & bitMasks[j]) == 0 ? -1L : 1L) * transform(j, bByte);
                    }
                }
                for (int j = ciphertextBitsPerByte - 1; j >= 0; j--) {
                    bitIndex--;
                    int byteIndex = totalBytes - 1 - (bitIndex / 8);
                    int bitOffset = bitIndex % 8;
                    if ((cipher & (1L << j)) != 0) {
                        blockCipherBytes[byteIndex] |= (byte) (1 << bitOffset);
                    }
                }
            }
            if (blockCipherBytes.length < ciphertextBytesPerBlock) {
                int diff = ciphertextBytesPerBlock - blockCipherBytes.length;
                for (int i = 0; i < diff; i++) {
                    result.put((byte) 0);
                }
                result.put(blockCipherBytes);
            } else {
                result.put(blockCipherBytes, blockCipherBytes.length - ciphertextBytesPerBlock, ciphertextBytesPerBlock);
            }
        }
        result.put((byte) padding);
        return result.array();
    }
    
    /**
     * Decrypt.
     *
     * @param ciphertext ciphertext
     * @return plaintext
     */
    public byte[] decrypt(final byte[] ciphertext) {
        int blockCount = (ciphertext.length - 1) / ciphertextBytesPerBlock;
        int plaintextSize = blockCount * plaintextBytesPerBlock - ciphertext[ciphertext.length - 1];
        ByteBuffer result = ByteBuffer.allocate(plaintextSize);
        ByteBuffer ciphertextBuffer = ByteBuffer.wrap(ciphertext);
        for (int block = 0; block < blockCount; block++) {
            byte[] blockBytes = new byte[ciphertextBytesPerBlock];
            ciphertextBuffer.get(blockBytes);
            int plaintextOffset = block * plaintextBytesPerBlock;
            int bitIndex = plaintextBytesPerBlock * ciphertextBitsPerByte;
            for (int i = plaintextBytesPerBlock - 1; i >= 0; i--) {
                long cipher = 0;
                for (int j = 0; j < ciphertextBitsPerByte; j++) {
                    bitIndex--;
                    int byteIndex = bitIndex / 8;
                    int bitOffset = 7 - bitIndex % 8;
                    int bit = blockBytes[byteIndex] >> bitOffset & 1;
                    cipher |= ((long) bit) << j;
                }
                int b = 0;
                long a = transform(0, 0);
                if (cipher >= a) {
                    b |= bitMasks[1];
                }
                for (int j = 1; j < 8; j++) {
                    long aj = transform(j, b);
                    a += ((b & bitMasks[j]) == 0L ? -1L : 1L) * aj;
                    if (cipher >= a) {
                        b |= bitMasks[j + 1];
                    }
                }
                if (plaintextOffset + i < plaintextSize) {
                    result.position(plaintextOffset + i);
                    result.put((byte) b);
                }
            }
        }
        return result.array();
    }
    
    private long transform(final int index, final int bByte) {
        return transformArray[getTransformKey(index, bByte)];
    }
    
    private long transform(final int index, final int bByte, final MessageDigest md) {
        int x = bByte;
        try {
            int shift = 8 - index;
            x >>= shift;
            x <<= shift;
            md.update(kBytes);
            md.update((byte) x);
            byte[] hash = md.digest();
            long value = 0;
            for (byte b : hash) {
                value = (value << 8) | (b & 0xFF);
            }
            return (Math.abs(value) % fRange[index]) + fMin[index];
            // CHECKSTYLE:OFF
        } catch (Exception ignored) {
            // CHECKSTYLE:ON
            return 0L;
        }
    }
    
    private long[] getTransformArray() {
        long[] result = new long[2304];
        MessageDigest md = DigestUtils.getSha256Digest();
        for (int bByte = 0; bByte <= 255; bByte++) {
            for (int index = 0; index <= 8; index++) {
                result[getTransformKey(index, bByte)] = transform(index, bByte, md);
            }
        }
        return result;
    }
    
    private int getTransformKey(final int index, final int bByte) {
        return index << 8 | bByte;
    }
}
