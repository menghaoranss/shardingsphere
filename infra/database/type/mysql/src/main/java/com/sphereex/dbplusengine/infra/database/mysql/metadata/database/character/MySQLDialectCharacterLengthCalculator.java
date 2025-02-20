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

package com.sphereex.dbplusengine.infra.database.mysql.metadata.database.character;

import com.cedarsoftware.util.CaseInsensitiveMap;
import com.cedarsoftware.util.CaseInsensitiveSet;
import com.sphereex.dbplusengine.infra.database.core.metadata.database.character.DialectCharacterLengthCalculator;
import org.apache.shardingsphere.infra.exception.core.ShardingSpherePreconditions;

import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * MySQL dialect character length calculator.
 */
public final class MySQLDialectCharacterLengthCalculator implements DialectCharacterLengthCalculator {
    
    private static final Map<String, Integer> CHARSET_BYTE_LENGTH_MAP = new CaseInsensitiveMap<>(8, 1F);
    
    private static final Map<String, Integer> DATA_TYPE_BYTE_LENGTH_MAP = new CaseInsensitiveMap<>(14, 1F);
    
    private static final Map<String, String> COLLATION_AND_CHARSET_MAP = new CaseInsensitiveMap<>(194, 1F);
    
    private static final Set<String> CHARACTER_TYPES = new CaseInsensitiveSet<>(Arrays.asList("VARCHAR", "CHAR"));
    
    private static final String DEFAULT_CHARSET = "utf8mb4";
    
    private static final int COLUMN_AND_ROW_BYTE_LIMIT = 65535;
    
    static {
        initCharsetByteLengthMap();
        initCollationAndCharsetMap();
        initDataTypeByteLengthMap();
    }
    
    @Override
    public boolean isNeedCalculate() {
        return true;
    }
    
    @Override
    public int getCharsetCharToByteRatio(final String charset) {
        return CHARSET_BYTE_LENGTH_MAP.containsKey(charset) ? CHARSET_BYTE_LENGTH_MAP.get(charset) : CHARSET_BYTE_LENGTH_MAP.get(DEFAULT_CHARSET);
    }
    
    @Override
    public String getCharsetNameByCollation(final String collation) {
        return COLLATION_AND_CHARSET_MAP.getOrDefault(collation, DEFAULT_CHARSET);
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
        ShardingSpherePreconditions.checkState(columnByteLength <= COLUMN_AND_ROW_BYTE_LIMIT,
                () -> new IllegalArgumentException(String.format("Column `%s` byte length exceeds the upper limit, current: %s, limit: %s", columnName, columnByteLength, COLUMN_AND_ROW_BYTE_LIMIT)));
    }
    
    @Override
    public void checkRowByteLength(final int rowByteLength) {
        ShardingSpherePreconditions.checkState(rowByteLength <= COLUMN_AND_ROW_BYTE_LIMIT,
                () -> new IllegalArgumentException(String.format("Row byte length exceeds the upper limit, current: %s, limit: %s", rowByteLength, COLUMN_AND_ROW_BYTE_LIMIT)));
    }
    
    @Override
    public int calculateColumnByteLength(final int columnCharLength, final boolean notNull, final String dataType, final String columnName, final String columnCharset,
                                         final AtomicBoolean isNullCalculated) {
        checkColumnCharLength(columnCharLength, dataType);
        int result;
        if (isCharacterType(dataType)) {
            int columnByteLength = columnCharLength * getCharsetCharToByteRatio(columnCharset);
            result = columnByteLength + getNullByteLength(notNull, isNullCalculated) + getVarcharExtraByteLength(dataType, columnByteLength);
        } else {
            result = getDataTypeByteLength(dataType);
        }
        checkColumnByteLength(result, columnName);
        return result;
    }
    
    @Override
    public int toCharacterLength(final int byteLength, final String charset) {
        Integer byteLengthRate = CHARSET_BYTE_LENGTH_MAP.containsKey(charset) ? CHARSET_BYTE_LENGTH_MAP.get(charset) : CHARSET_BYTE_LENGTH_MAP.get(DEFAULT_CHARSET);
        return 0 == byteLength % byteLengthRate ? byteLength / byteLengthRate : byteLength / byteLengthRate + 1;
    }
    
    @Override
    public boolean isSupportedColumnCharacterSetDefinition() {
        return true;
    }
    
    @Override
    public String getDefaultColumnLengthUnit() {
        return "CHAR";
    }
    
    private static int getNullByteLength(final boolean notNull, final AtomicBoolean isNullCalculated) {
        if (notNull || Boolean.TRUE.equals(isNullCalculated.get())) {
            return 0;
        }
        isNullCalculated.set(Boolean.TRUE);
        return 1;
    }
    
    private static void checkColumnCharLength(final int columnCharLength, final String dataType) {
        ShardingSpherePreconditions.checkState(!"CHAR".equalsIgnoreCase(dataType) || columnCharLength <= 255,
                () -> new IllegalArgumentException(String.format("Column length too big for column '%s' (max = 255); use BLOB or TEXT instead", columnCharLength)));
    }
    
    private static int getVarcharExtraByteLength(final String dataType, final int columnByteLength) {
        if ("VARCHAR".equalsIgnoreCase(dataType)) {
            return columnByteLength <= 255 ? 1 : 2;
        }
        return 0;
    }
    
    private int getDataTypeByteLength(final String dataType) {
        return DATA_TYPE_BYTE_LENGTH_MAP.getOrDefault(dataType, 0);
    }
    
    @Override
    public String getDatabaseType() {
        return "MySQL";
    }
    
    private static void initCharsetByteLengthMap() {
        CHARSET_BYTE_LENGTH_MAP.put("ascii", 1);
        CHARSET_BYTE_LENGTH_MAP.put("latin1", 1);
        CHARSET_BYTE_LENGTH_MAP.put("gb2312", 2);
        CHARSET_BYTE_LENGTH_MAP.put("gbk", 2);
        CHARSET_BYTE_LENGTH_MAP.put("utf8mb3", 3);
        CHARSET_BYTE_LENGTH_MAP.put("utf8mb4", 4);
        CHARSET_BYTE_LENGTH_MAP.put("utf16", 4);
        CHARSET_BYTE_LENGTH_MAP.put("utf32", 4);
    }
    
    private static void initCollationAndCharsetMap() {
        initUTF8Collations();
        initUTF16Collations();
        initUTF32Collations();
        initOtherCollations();
    }
    
    private static void initUTF8Collations() {
        COLLATION_AND_CHARSET_MAP.put("utf8mb3_general_ci", "utf8mb3");
        COLLATION_AND_CHARSET_MAP.put("utf8mb3_tolower_ci", "utf8mb3");
        COLLATION_AND_CHARSET_MAP.put("utf8mb3_bin", "utf8mb3");
        COLLATION_AND_CHARSET_MAP.put("utf8mb3_unicode_ci", "utf8mb3");
        COLLATION_AND_CHARSET_MAP.put("utf8mb3_icelandic_ci", "utf8mb3");
        COLLATION_AND_CHARSET_MAP.put("utf8mb3_latvian_ci", "utf8mb3");
        COLLATION_AND_CHARSET_MAP.put("utf8mb3_romanian_ci", "utf8mb3");
        COLLATION_AND_CHARSET_MAP.put("utf8mb3_slovenian_ci", "utf8mb3");
        COLLATION_AND_CHARSET_MAP.put("utf8mb3_polish_ci", "utf8mb3");
        COLLATION_AND_CHARSET_MAP.put("utf8mb3_estonian_ci", "utf8mb3");
        COLLATION_AND_CHARSET_MAP.put("utf8mb3_spanish_ci", "utf8mb3");
        COLLATION_AND_CHARSET_MAP.put("utf8mb3_swedish_ci", "utf8mb3");
        COLLATION_AND_CHARSET_MAP.put("utf8mb3_turkish_ci", "utf8mb3");
        COLLATION_AND_CHARSET_MAP.put("utf8mb3_czech_ci", "utf8mb3");
        COLLATION_AND_CHARSET_MAP.put("utf8mb3_danish_ci", "utf8mb3");
        COLLATION_AND_CHARSET_MAP.put("utf8mb3_lithuanian_ci", "utf8mb3");
        COLLATION_AND_CHARSET_MAP.put("utf8mb3_slovak_ci", "utf8mb3");
        COLLATION_AND_CHARSET_MAP.put("utf8mb3_spanish2_ci", "utf8mb3");
        COLLATION_AND_CHARSET_MAP.put("utf8mb3_roman_ci", "utf8mb3");
        COLLATION_AND_CHARSET_MAP.put("utf8mb3_persian_ci", "utf8mb3");
        COLLATION_AND_CHARSET_MAP.put("utf8mb3_esperanto_ci", "utf8mb3");
        COLLATION_AND_CHARSET_MAP.put("utf8mb3_hungarian_ci", "utf8mb3");
        COLLATION_AND_CHARSET_MAP.put("utf8mb3_sinhala_ci", "utf8mb3");
        COLLATION_AND_CHARSET_MAP.put("utf8mb3_german2_ci", "utf8mb3");
        COLLATION_AND_CHARSET_MAP.put("utf8mb3_croatian_ci", "utf8mb3");
        COLLATION_AND_CHARSET_MAP.put("utf8mb3_unicode_520_ci", "utf8mb3");
        COLLATION_AND_CHARSET_MAP.put("utf8mb3_vietnamese_ci", "utf8mb3");
        COLLATION_AND_CHARSET_MAP.put("utf8mb3_general_mysql500_ci", "utf8mb3");
        COLLATION_AND_CHARSET_MAP.put("utf8mb4_general_ci", "utf8mb4");
        COLLATION_AND_CHARSET_MAP.put("utf8mb4_bin", "utf8mb4");
        COLLATION_AND_CHARSET_MAP.put("utf8mb4_unicode_ci", "utf8mb4");
        COLLATION_AND_CHARSET_MAP.put("utf8mb4_icelandic_ci", "utf8mb4");
        COLLATION_AND_CHARSET_MAP.put("utf8mb4_latvian_ci", "utf8mb4");
        COLLATION_AND_CHARSET_MAP.put("utf8mb4_romanian_ci", "utf8mb4");
        COLLATION_AND_CHARSET_MAP.put("utf8mb4_slovenian_ci", "utf8mb4");
        COLLATION_AND_CHARSET_MAP.put("utf8mb4_polish_ci", "utf8mb4");
        COLLATION_AND_CHARSET_MAP.put("utf8mb4_estonian_ci", "utf8mb4");
        COLLATION_AND_CHARSET_MAP.put("utf8mb4_spanish_ci", "utf8mb4");
        COLLATION_AND_CHARSET_MAP.put("utf8mb4_swedish_ci", "utf8mb4");
        COLLATION_AND_CHARSET_MAP.put("utf8mb4_turkish_ci", "utf8mb4");
        COLLATION_AND_CHARSET_MAP.put("utf8mb4_czech_ci", "utf8mb4");
        COLLATION_AND_CHARSET_MAP.put("utf8mb4_danish_ci", "utf8mb4");
        COLLATION_AND_CHARSET_MAP.put("utf8mb4_lithuanian_ci", "utf8mb4");
        COLLATION_AND_CHARSET_MAP.put("utf8mb4_slovak_ci", "utf8mb4");
        COLLATION_AND_CHARSET_MAP.put("utf8mb4_spanish2_ci", "utf8mb4");
        COLLATION_AND_CHARSET_MAP.put("utf8mb4_roman_ci", "utf8mb4");
        COLLATION_AND_CHARSET_MAP.put("utf8mb4_persian_ci", "utf8mb4");
        COLLATION_AND_CHARSET_MAP.put("utf8mb4_esperanto_ci", "utf8mb4");
        COLLATION_AND_CHARSET_MAP.put("utf8mb4_hungarian_ci", "utf8mb4");
        COLLATION_AND_CHARSET_MAP.put("utf8mb4_sinhala_ci", "utf8mb4");
        COLLATION_AND_CHARSET_MAP.put("utf8mb4_german2_ci", "utf8mb4");
        COLLATION_AND_CHARSET_MAP.put("utf8mb4_croatian_ci", "utf8mb4");
        COLLATION_AND_CHARSET_MAP.put("utf8mb4_unicode_520_ci", "utf8mb4");
        COLLATION_AND_CHARSET_MAP.put("utf8mb4_vietnamese_ci", "utf8mb4");
        COLLATION_AND_CHARSET_MAP.put("utf8mb4_0900_ai_ci", "utf8mb4");
        COLLATION_AND_CHARSET_MAP.put("utf8mb4_de_pb_0900_ai_ci", "utf8mb4");
        COLLATION_AND_CHARSET_MAP.put("utf8mb4_is_0900_ai_ci", "utf8mb4");
        COLLATION_AND_CHARSET_MAP.put("utf8mb4_lv_0900_ai_ci", "utf8mb4");
        COLLATION_AND_CHARSET_MAP.put("utf8mb4_ro_0900_ai_ci", "utf8mb4");
        COLLATION_AND_CHARSET_MAP.put("utf8mb4_sl_0900_ai_ci", "utf8mb4");
        COLLATION_AND_CHARSET_MAP.put("utf8mb4_pl_0900_ai_ci", "utf8mb4");
        COLLATION_AND_CHARSET_MAP.put("utf8mb4_et_0900_ai_ci", "utf8mb4");
        COLLATION_AND_CHARSET_MAP.put("utf8mb4_es_0900_ai_ci", "utf8mb4");
        COLLATION_AND_CHARSET_MAP.put("utf8mb4_sv_0900_ai_ci", "utf8mb4");
        COLLATION_AND_CHARSET_MAP.put("utf8mb4_tr_0900_ai_ci", "utf8mb4");
        COLLATION_AND_CHARSET_MAP.put("utf8mb4_cs_0900_ai_ci", "utf8mb4");
        COLLATION_AND_CHARSET_MAP.put("utf8mb4_da_0900_ai_ci", "utf8mb4");
        COLLATION_AND_CHARSET_MAP.put("utf8mb4_lt_0900_ai_ci", "utf8mb4");
        COLLATION_AND_CHARSET_MAP.put("utf8mb4_sk_0900_ai_ci", "utf8mb4");
        COLLATION_AND_CHARSET_MAP.put("utf8mb4_es_trad_0900_ai_ci", "utf8mb4");
        COLLATION_AND_CHARSET_MAP.put("utf8mb4_la_0900_ai_ci", "utf8mb4");
        COLLATION_AND_CHARSET_MAP.put("utf8mb4_eo_0900_ai_ci", "utf8mb4");
        COLLATION_AND_CHARSET_MAP.put("utf8mb4_hu_0900_ai_ci", "utf8mb4");
        COLLATION_AND_CHARSET_MAP.put("utf8mb4_hr_0900_ai_ci", "utf8mb4");
        COLLATION_AND_CHARSET_MAP.put("utf8mb4_vi_0900_ai_ci", "utf8mb4");
        COLLATION_AND_CHARSET_MAP.put("utf8mb4_0900_as_cs", "utf8mb4");
        COLLATION_AND_CHARSET_MAP.put("utf8mb4_de_pb_0900_as_cs", "utf8mb4");
        COLLATION_AND_CHARSET_MAP.put("utf8mb4_is_0900_as_cs", "utf8mb4");
        COLLATION_AND_CHARSET_MAP.put("utf8mb4_lv_0900_as_cs", "utf8mb4");
        COLLATION_AND_CHARSET_MAP.put("utf8mb4_ro_0900_as_cs", "utf8mb4");
        COLLATION_AND_CHARSET_MAP.put("utf8mb4_sl_0900_as_cs", "utf8mb4");
        COLLATION_AND_CHARSET_MAP.put("utf8mb4_pl_0900_as_cs", "utf8mb4");
        COLLATION_AND_CHARSET_MAP.put("utf8mb4_et_0900_as_cs", "utf8mb4");
        COLLATION_AND_CHARSET_MAP.put("utf8mb4_es_0900_as_cs", "utf8mb4");
        COLLATION_AND_CHARSET_MAP.put("utf8mb4_sv_0900_as_cs", "utf8mb4");
        COLLATION_AND_CHARSET_MAP.put("utf8mb4_tr_0900_as_cs", "utf8mb4");
        COLLATION_AND_CHARSET_MAP.put("utf8mb4_cs_0900_as_cs", "utf8mb4");
        COLLATION_AND_CHARSET_MAP.put("utf8mb4_da_0900_as_cs", "utf8mb4");
        COLLATION_AND_CHARSET_MAP.put("utf8mb4_lt_0900_as_cs", "utf8mb4");
        COLLATION_AND_CHARSET_MAP.put("utf8mb4_sk_0900_as_cs", "utf8mb4");
        COLLATION_AND_CHARSET_MAP.put("utf8mb4_es_trad_0900_as_cs", "utf8mb4");
        COLLATION_AND_CHARSET_MAP.put("utf8mb4_la_0900_as_cs", "utf8mb4");
        COLLATION_AND_CHARSET_MAP.put("utf8mb4_eo_0900_as_cs", "utf8mb4");
        COLLATION_AND_CHARSET_MAP.put("utf8mb4_hu_0900_as_cs", "utf8mb4");
        COLLATION_AND_CHARSET_MAP.put("utf8mb4_hr_0900_as_cs", "utf8mb4");
        COLLATION_AND_CHARSET_MAP.put("utf8mb4_vi_0900_as_cs", "utf8mb4");
        COLLATION_AND_CHARSET_MAP.put("utf8mb4_ja_0900_as_cs", "utf8mb4");
        COLLATION_AND_CHARSET_MAP.put("utf8mb4_ja_0900_as_cs_ks", "utf8mb4");
        COLLATION_AND_CHARSET_MAP.put("utf8mb4_0900_as_ci", "utf8mb4");
        COLLATION_AND_CHARSET_MAP.put("utf8mb4_ru_0900_ai_ci", "utf8mb4");
        COLLATION_AND_CHARSET_MAP.put("utf8mb4_ru_0900_as_cs", "utf8mb4");
        COLLATION_AND_CHARSET_MAP.put("utf8mb4_zh_0900_as_cs", "utf8mb4");
        COLLATION_AND_CHARSET_MAP.put("utf8mb4_0900_bin", "utf8mb4");
        COLLATION_AND_CHARSET_MAP.put("utf8mb4_nb_0900_ai_ci", "utf8mb4");
        COLLATION_AND_CHARSET_MAP.put("utf8mb4_nb_0900_as_cs", "utf8mb4");
        COLLATION_AND_CHARSET_MAP.put("utf8mb4_nn_0900_ai_ci", "utf8mb4");
        COLLATION_AND_CHARSET_MAP.put("utf8mb4_nn_0900_as_cs", "utf8mb4");
        COLLATION_AND_CHARSET_MAP.put("utf8mb4_sr_latn_0900_ai_ci", "utf8mb4");
        COLLATION_AND_CHARSET_MAP.put("utf8mb4_sr_latn_0900_as_cs", "utf8mb4");
        COLLATION_AND_CHARSET_MAP.put("utf8mb4_bs_0900_ai_ci", "utf8mb4");
        COLLATION_AND_CHARSET_MAP.put("utf8mb4_bs_0900_as_cs", "utf8mb4");
        COLLATION_AND_CHARSET_MAP.put("utf8mb4_bg_0900_ai_ci", "utf8mb4");
        COLLATION_AND_CHARSET_MAP.put("utf8mb4_bg_0900_as_cs", "utf8mb4");
        COLLATION_AND_CHARSET_MAP.put("utf8mb4_gl_0900_ai_ci", "utf8mb4");
        COLLATION_AND_CHARSET_MAP.put("utf8mb4_gl_0900_as_cs", "utf8mb4");
        COLLATION_AND_CHARSET_MAP.put("utf8mb4_mn_cyrl_0900_ai_ci", "utf8mb4");
        COLLATION_AND_CHARSET_MAP.put("utf8mb4_mn_cyrl_0900_as_cs", "utf8mb4");
    }
    
    private static void initUTF16Collations() {
        COLLATION_AND_CHARSET_MAP.put("utf16_general_ci", "utf16");
        COLLATION_AND_CHARSET_MAP.put("utf16_bin", "utf16");
        COLLATION_AND_CHARSET_MAP.put("utf16_unicode_ci", "utf16");
        COLLATION_AND_CHARSET_MAP.put("utf16_icelandic_ci", "utf16");
        COLLATION_AND_CHARSET_MAP.put("utf16_latvian_ci", "utf16");
        COLLATION_AND_CHARSET_MAP.put("utf16_romanian_ci", "utf16");
        COLLATION_AND_CHARSET_MAP.put("utf16_slovenian_ci", "utf16");
        COLLATION_AND_CHARSET_MAP.put("utf16_polish_ci", "utf16");
        COLLATION_AND_CHARSET_MAP.put("utf16_estonian_ci", "utf16");
        COLLATION_AND_CHARSET_MAP.put("utf16_spanish_ci", "utf16");
        COLLATION_AND_CHARSET_MAP.put("utf16_swedish_ci", "utf16");
        COLLATION_AND_CHARSET_MAP.put("utf16_turkish_ci", "utf16");
        COLLATION_AND_CHARSET_MAP.put("utf16_czech_ci", "utf16");
        COLLATION_AND_CHARSET_MAP.put("utf16_danish_ci", "utf16");
        COLLATION_AND_CHARSET_MAP.put("utf16_lithuanian_ci", "utf16");
        COLLATION_AND_CHARSET_MAP.put("utf16_slovak_ci", "utf16");
        COLLATION_AND_CHARSET_MAP.put("utf16_spanish2_ci", "utf16");
        COLLATION_AND_CHARSET_MAP.put("utf16_roman_ci", "utf16");
        COLLATION_AND_CHARSET_MAP.put("utf16_persian_ci", "utf16");
        COLLATION_AND_CHARSET_MAP.put("utf16_esperanto_ci", "utf16");
        COLLATION_AND_CHARSET_MAP.put("utf16_hungarian_ci", "utf16");
        COLLATION_AND_CHARSET_MAP.put("utf16_sinhala_ci", "utf16");
        COLLATION_AND_CHARSET_MAP.put("utf16_german2_ci", "utf16");
        COLLATION_AND_CHARSET_MAP.put("utf16_croatian_ci", "utf16");
        COLLATION_AND_CHARSET_MAP.put("utf16_unicode_520_ci", "utf16");
        COLLATION_AND_CHARSET_MAP.put("utf16_vietnamese_ci", "utf16");
    }
    
    private static void initUTF32Collations() {
        COLLATION_AND_CHARSET_MAP.put("utf32_general_ci", "utf32");
        COLLATION_AND_CHARSET_MAP.put("utf32_bin", "utf32");
        COLLATION_AND_CHARSET_MAP.put("utf32_unicode_ci", "utf32");
        COLLATION_AND_CHARSET_MAP.put("utf32_icelandic_ci", "utf32");
        COLLATION_AND_CHARSET_MAP.put("utf32_latvian_ci", "utf32");
        COLLATION_AND_CHARSET_MAP.put("utf32_romanian_ci", "utf32");
        COLLATION_AND_CHARSET_MAP.put("utf32_slovenian_ci", "utf32");
        COLLATION_AND_CHARSET_MAP.put("utf32_polish_ci", "utf32");
        COLLATION_AND_CHARSET_MAP.put("utf32_estonian_ci", "utf32");
        COLLATION_AND_CHARSET_MAP.put("utf32_spanish_ci", "utf32");
        COLLATION_AND_CHARSET_MAP.put("utf32_swedish_ci", "utf32");
        COLLATION_AND_CHARSET_MAP.put("utf32_turkish_ci", "utf32");
        COLLATION_AND_CHARSET_MAP.put("utf32_czech_ci", "utf32");
        COLLATION_AND_CHARSET_MAP.put("utf32_danish_ci", "utf32");
        COLLATION_AND_CHARSET_MAP.put("utf32_lithuanian_ci", "utf32");
        COLLATION_AND_CHARSET_MAP.put("utf32_slovak_ci", "utf32");
        COLLATION_AND_CHARSET_MAP.put("utf32_spanish2_ci", "utf32");
        COLLATION_AND_CHARSET_MAP.put("utf32_roman_ci", "utf32");
        COLLATION_AND_CHARSET_MAP.put("utf32_persian_ci", "utf32");
        COLLATION_AND_CHARSET_MAP.put("utf32_esperanto_ci", "utf32");
        COLLATION_AND_CHARSET_MAP.put("utf32_hungarian_ci", "utf32");
        COLLATION_AND_CHARSET_MAP.put("utf32_sinhala_ci", "utf32");
        COLLATION_AND_CHARSET_MAP.put("utf32_german2_ci", "utf32");
        COLLATION_AND_CHARSET_MAP.put("utf32_croatian_ci", "utf32");
        COLLATION_AND_CHARSET_MAP.put("utf32_unicode_520_ci", "utf32");
        COLLATION_AND_CHARSET_MAP.put("utf32_vietnamese_ci", "utf32");
    }
    
    private static void initOtherCollations() {
        COLLATION_AND_CHARSET_MAP.put("ascii_general_ci", "ascii");
        COLLATION_AND_CHARSET_MAP.put("ascii_bin", "ascii");
        COLLATION_AND_CHARSET_MAP.put("gb2312_chinese_ci", "gb2312");
        COLLATION_AND_CHARSET_MAP.put("gb2312_bin", "gb2312");
        COLLATION_AND_CHARSET_MAP.put("gbk_chinese_ci", "gbk");
        COLLATION_AND_CHARSET_MAP.put("gbk_bin", "gbk");
        COLLATION_AND_CHARSET_MAP.put("latin1_german1_ci", "latin1");
        COLLATION_AND_CHARSET_MAP.put("latin1_swedish_ci", "latin1");
        COLLATION_AND_CHARSET_MAP.put("latin1_danish_ci", "latin1");
        COLLATION_AND_CHARSET_MAP.put("latin1_german2_ci", "latin1");
        COLLATION_AND_CHARSET_MAP.put("latin1_bin", "latin1");
        COLLATION_AND_CHARSET_MAP.put("latin1_general_ci", "latin1");
        COLLATION_AND_CHARSET_MAP.put("latin1_general_cs", "latin1");
        COLLATION_AND_CHARSET_MAP.put("latin1_spanish_ci", "latin1");
    }
    
    private static void initDataTypeByteLengthMap() {
        DATA_TYPE_BYTE_LENGTH_MAP.put("TINYINT", 1);
        DATA_TYPE_BYTE_LENGTH_MAP.put("SMALLINT", 2);
        DATA_TYPE_BYTE_LENGTH_MAP.put("MEDIUMINT", 3);
        DATA_TYPE_BYTE_LENGTH_MAP.put("INT", 4);
        DATA_TYPE_BYTE_LENGTH_MAP.put("BIGINT", 8);
        DATA_TYPE_BYTE_LENGTH_MAP.put("FLOAT", 4);
        DATA_TYPE_BYTE_LENGTH_MAP.put("DOUBLE", 8);
        DATA_TYPE_BYTE_LENGTH_MAP.put("DECIMAL", 65);
        DATA_TYPE_BYTE_LENGTH_MAP.put("DATE", 3);
        DATA_TYPE_BYTE_LENGTH_MAP.put("TIME", 3);
        DATA_TYPE_BYTE_LENGTH_MAP.put("DATETIME", 8);
        DATA_TYPE_BYTE_LENGTH_MAP.put("TIMESTAMP", 4);
        DATA_TYPE_BYTE_LENGTH_MAP.put("YEAR", 1);
        DATA_TYPE_BYTE_LENGTH_MAP.put("TEXT", 10);
    }
}
