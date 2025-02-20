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

package com.sphereex.dbplusengine.mask.algorithm.cover;

import org.apache.shardingsphere.mask.spi.MaskAlgorithm;

import java.util.Properties;

/**
 * SphereEx chinese name mask algorithm.
 */
public final class SphereExChineseNameMaskAlgorithm implements MaskAlgorithm<Object, String> {
    
    private static final String REPLACE_CHAR = "replace-char";
    
    private static final char DEFAULT_REPLACED_CHAR = '*';
    
    private Character replaceChar;
    
    @Override
    public void init(final Properties props) {
        replaceChar = createReplaceChar(props);
    }
    
    private Character createReplaceChar(final Properties props) {
        if (props.containsKey(REPLACE_CHAR)) {
            return props.getProperty(REPLACE_CHAR).charAt(0);
        }
        return DEFAULT_REPLACED_CHAR;
    }
    
    @Override
    public String mask(final Object plainValue) {
        if (null == plainValue) {
            return null;
        }
        return doMask(String.valueOf(plainValue), 1, 1, 1, replaceChar);
    }
    
    private String doMask(final String plainValue, final int startInclude, final int endExclude, final int minMaskSize, final char replacedChar) {
        if (null == plainValue || 1 >= plainValue.length()) {
            return plainValue;
        }
        if (startInclude > plainValue.length()) {
            return plainValue;
        }
        if (startInclude > endExclude) {
            return plainValue;
        }
        int endIndex = plainValue.length() - endExclude;
        int maskSize = 0;
        StringBuilder result = new StringBuilder(plainValue.length());
        for (int i = 0; i < plainValue.length(); i++) {
            if (i >= startInclude && i < endIndex) {
                result.append(replacedChar);
                maskSize++;
            } else {
                result.append(plainValue.charAt(i));
            }
        }
        for (int i = endIndex; i < plainValue.length(); i++) {
            if (maskSize < minMaskSize) {
                result.setCharAt(i, replacedChar);
                maskSize++;
            } else {
                break;
            }
        }
        return result.toString();
    }
    
    @Override
    public String getType() {
        return "SphereEx:MASK_CHINESE_NAME";
    }
}
