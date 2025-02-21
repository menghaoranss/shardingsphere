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

package com.sphereex.dbplusengine.test.e2e.engine.hook;

import com.sphereex.dbplusengine.test.e2e.engine.context.ComparisonDataSourceContext;

/**
 * Sql handler.
 */
public interface SqlExecuteHook {
    
    /**
     * Test.
     *
     * @param caseId case id
     * @param sql sql
     * @param context context
     * @return whether execute test
     */
    default boolean test(final String caseId, final String sql, final ComparisonDataSourceContext context) {
        return true;
    }
    
    /**
     * Before.
     *
     * @param sql sql
     * @param context context
     * @return the SQL that may have been modified
     */
    default String before(String sql, ComparisonDataSourceContext context) {
        return sql;
    }
    
    /**
     * After.
     *
     * @param sql sql
     * @param context context
     */
    default void after(String sql, ComparisonDataSourceContext context) {
    }
}
