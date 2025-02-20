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

import com.sphereex.dbplusengine.encrypt.algorithm.ope.cipher.FastOpeCipher;
import com.sphereex.dbplusengine.encrypt.algorithm.ope.cipher.OpeCipher;
import lombok.RequiredArgsConstructor;

import java.nio.ByteBuffer;

/**
 * Fast ope cipher generator.
 */
@RequiredArgsConstructor
public final class FastOpeCipherGenerator implements OpeCipherGenerator {
    
    private static final int DEFAULT_TAU = 16;
    
    private final double alphaKey;
    
    private final double factorEKey;
    
    private final long factorKKey;
    
    /**
     * Generate.
     *
     * @return ope cipher
     */
    public OpeCipher generate() {
        double alpha = alphaKey / 2.0;
        double beta = 1.0 - alpha;
        double factorE = factorEKey * alpha;
        long factorN = (long) Math.ceil(DEFAULT_TAU / (beta * Math.pow(factorE, 8D)));
        long factorK = factorKKey & 0x7fffffffffffffffL;
        return new FastOpeCipher(factorN, alpha, factorE, factorK);
    }
    
    /**
     * Decode cipher.
     *
     * @param bytes bytes
     * @return OPE cipher
     */
    public OpeCipher decodeCipher(final byte[] bytes) {
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        long factorN = buffer.getLong();
        double alpha = buffer.getDouble();
        double factorE = buffer.getDouble();
        long factorK = buffer.getLong();
        return new FastOpeCipher(factorN, alpha, factorE, factorK);
    }
}
