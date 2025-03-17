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

package com.sphereex.dbplusengine.encrypt.constant;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

/**
 * Encrypt mode key.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class EncryptModeKey {
    
    public static final String UDF_SQL_ENABLED_KEY = "udf-sql-enabled";
    
    public static final String UDF_ROUTINE_ENABLED_KEY = "udf-routine-enabled";
    
    public static final String UDF_VIEW_ENABLED_KEY = "udf-view-enabled";
    
    public static final String USE_ORIGINAL_SQL_WHEN_CIPHER_QUERY_FAILED_KEY = "use-original-sql-when-cipher-query-failed";
    
    // TODO BEGIN: remove derived suffix when encrypt support rewrite based ast, and generate derived column accrding to unique column name
    // https://github.com/SphereEx/dbplus-engine/issues/10925
    public static final String DERIVED_CIPHER_SUFFIX_KEY = "derived-cipher-suffix";
    
    public static final String DERIVED_ASSISTED_QUERY_SUFFIX_KEY = "derived-assisted-query-suffix";
    
    public static final String DERIVED_LIKE_QUERY_SUFFIX_KEY = "derived-like-query-suffix";
    
    public static final String DERIVED_ORDER_QUERY_SUFFIX_KEY = "derived-order-query-suffix";
    // TODO END
    
    public static final String RENAME_TABLE_PREFIX_KEY = "rename-table-prefix";
}
