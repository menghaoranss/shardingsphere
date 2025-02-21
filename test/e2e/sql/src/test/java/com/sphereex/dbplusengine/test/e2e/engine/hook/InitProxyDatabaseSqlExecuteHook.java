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
import com.sphereex.dbplusengine.test.e2e.engine.context.ProxyComparisonDataSourceContext;
import org.apache.shardingsphere.sql.parser.statement.core.statement.SQLStatement;
import org.apache.shardingsphere.sql.parser.statement.core.statement.ddl.CreateDatabaseStatement;

public class InitProxyDatabaseSqlExecuteHook extends AbstratParserSqlExecuteHook {
    
    @Override
    public void after(final String sql, final ComparisonDataSourceContext context) {
        SQLStatement sqlStatement = getSQLParserEngine(context.getDatabaseType()).parse(sql, true);
        if (!(sqlStatement instanceof CreateDatabaseStatement)) {
            return;
        }
        String databaseName = ((CreateDatabaseStatement) sqlStatement).getDatabaseName();
        ((ProxyComparisonDataSourceContext) context).initProxyDatabase(databaseName);
    }
}
