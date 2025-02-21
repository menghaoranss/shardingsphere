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
import lombok.extern.slf4j.Slf4j;
import org.apache.shardingsphere.infra.database.core.metadata.database.system.DialectSystemDatabase;
import org.apache.shardingsphere.infra.database.core.spi.DatabaseTypedSPILoader;

import java.util.Collection;
import java.util.Collections;

@Slf4j
public class SkipSystemSchemaSqlExecuteHook implements SqlExecuteHook {
    
    @Override
    public boolean test(final String caseId, final String sql, final ComparisonDataSourceContext context) {
        Collection<String> systemSchemas = DatabaseTypedSPILoader.findService(DialectSystemDatabase.class, context.getDatabaseType())
                .map(DialectSystemDatabase::getSystemSchemas).orElse(Collections.emptyList());
        if (systemSchemas.stream().anyMatch(sql::contains)) {
            log.info("Skip test.");
            return false;
        }
        return true;
    }
}
