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

package com.sphereex.dbplusengine.encrypt.fixture;

import com.cedarsoftware.util.CaseInsensitiveSet;
import com.sphereex.dbplusengine.infra.database.core.metadata.database.character.DialectCharacterLengthCalculator;

import java.util.Arrays;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Character length calculator fixture.
 */
public final class CharacterLengthCalculatorFixture implements DialectCharacterLengthCalculator {
    
    private static final Set<String> CHARACTER_TYPES = new CaseInsensitiveSet<>(Arrays.asList("VARCHAR", "CHAR"));
    
    private static final String DEFAULT_CHARSET = "utf8mb4";
    
    @Override
    public boolean isNeedCalculate() {
        return false;
    }
    
    @Override
    public int getCharsetCharToByteRatio(final String charset) {
        return 1;
    }
    
    @Override
    public String getCharsetNameByCollation(final String collation) {
        return DEFAULT_CHARSET;
    }
    
    @Override
    public boolean isCharacterType(final String type) {
        return CHARACTER_TYPES.contains(type);
    }
    
    @Override
    public String getDefaultCharsetName() {
        return DEFAULT_CHARSET;
    }
    
    @Override
    public void checkColumnByteLength(final int columnByteLength, final String columnName) {
    }
    
    @Override
    public void checkRowByteLength(final int rowByteLength) {
    }
    
    @Override
    public int calculateColumnByteLength(final int columnCharLength, final boolean notNull, final String dataType,
                                         final String columnName, final String columnCharset, final AtomicBoolean isNullCalculated) {
        return columnCharLength;
    }
    
    @Override
    public int toCharacterLength(final int byteLength, final String charset) {
        return 0;
    }
    
    @Override
    public boolean isSupportedColumnCharacterSetDefinition() {
        return false;
    }
    
    @Override
    public String getDefaultColumnLengthUnit() {
        return null;
    }
    
    @Override
    public String getDatabaseType() {
        return "FIXTURE";
    }
    
    @Override
    public boolean isDefault() {
        return true;
    }
}
