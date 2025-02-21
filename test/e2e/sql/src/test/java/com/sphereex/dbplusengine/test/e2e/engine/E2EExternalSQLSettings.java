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

package com.sphereex.dbplusengine.test.e2e.engine;

import com.sphereex.dbplusengine.test.e2e.engine.hook.SqlExecuteHook;
import org.apache.shardingsphere.test.e2e.env.container.atomic.enums.AdapterType;
import org.apache.shardingsphere.test.loader.TestParameterLoadTemplate;

import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

/**
 * E2E external sql settings.
 */
@Inherited
@Retention(RetentionPolicy.RUNTIME)
public @interface E2EExternalSQLSettings {
    
    /**
     * Get default database.
     *
     * @return default database
     */
    String defaultDatabase() default "";
    
    /**
     * Get case URL.
     *
     * @return case URL
     */
    String caseURL();
    
    /**
     * Get result URL.
     *
     * @return case URL
     */
    String resultURL() default "";
    
    /**
     * Get databaseType.
     *
     * @return database type
     */
    String databaseType();
    
    /**
     * Report type.
     *
     * @return get report type
     */
    String reportType() default "CSV";
    
    /**
     * Get case loader.
     *
     * @return case loader
     */
    Class<? extends TestParameterLoadTemplate> template();
    
    /**
     * Get source type.
     *
     * @return source type
     */
    SourceType sourceType() default SourceType.GITHUB;
    
    /**
     * Get sql handler.
     *
     * @return sql handler
     */
    Class<? extends SqlExecuteHook>[] sqlHandler();
    
    /**
     * Get adapter type.
     *
     * @return adapter type
     */
    AdapterType adapterType() default AdapterType.JDBC;
    
    enum SourceType {
        GITHUB, LOCAL
    }
}
