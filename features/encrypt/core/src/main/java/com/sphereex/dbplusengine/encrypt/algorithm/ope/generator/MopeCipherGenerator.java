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

package com.sphereex.dbplusengine.encrypt.algorithm.ope.generator;

import com.sphereex.dbplusengine.encrypt.algorithm.ope.cipher.MopeCipher;
import com.sphereex.dbplusengine.encrypt.algorithm.ope.cipher.OpeCipher;
import lombok.Getter;

import java.math.BigInteger;
import java.nio.ByteBuffer;

/**
 * Mope cipher generator.
 */
public final class MopeCipherGenerator implements OpeCipherGenerator {
    
    private static final int DEFAULT_PLAINTEXT_BYTES = 8;
    
    @Getter
    private final FastOpeCipherGenerator generator;
    
    private final BigInteger offset;
    
    private final int plaintextBytes;
    
    public MopeCipherGenerator(final FastOpeCipherGenerator cipher, final int plaintextBytes, final BigInteger offset) {
        generator = cipher;
        this.plaintextBytes = plaintextBytes > 0 ? plaintextBytes : DEFAULT_PLAINTEXT_BYTES;
        this.offset = offset;
    }
    
    /**
     * Generate.
     *
     * @return OpeCipher
     */
    public OpeCipher generate() {
        return new MopeCipher(generator.generate(), plaintextBytes, offset);
    }
    
    /**
     * Decode cipher.
     *
     * @param bytes bytes
     * @return OpeCipher
     */
    public OpeCipher decodeCipher(final byte[] bytes) {
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        int plaintextBytes = buffer.getInt();
        byte[] offsetBytes = new byte[plaintextBytes + 1];
        buffer.get(offsetBytes, 1, plaintextBytes);
        BigInteger offset = new BigInteger(offsetBytes);
        byte[] keyBytes = new byte[bytes.length - buffer.position()];
        buffer.get(keyBytes);
        OpeCipher key = generator.decodeCipher(keyBytes);
        return new MopeCipher(key, plaintextBytes, offset);
    }
}
