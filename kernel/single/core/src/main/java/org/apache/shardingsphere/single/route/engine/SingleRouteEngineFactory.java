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

package org.apache.shardingsphere.single.route.engine;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.apache.shardingsphere.infra.hint.HintValueContext;
import org.apache.shardingsphere.infra.metadata.database.schema.QualifiedTable;
import org.apache.shardingsphere.sql.parser.statement.core.statement.SQLStatement;
import org.apache.shardingsphere.sql.parser.statement.core.statement.ddl.AlterSchemaStatement;
import org.apache.shardingsphere.sql.parser.statement.core.statement.ddl.CreateSchemaStatement;
import org.apache.shardingsphere.sql.parser.statement.core.statement.ddl.DropSchemaStatement;

import java.util.Collection;
import java.util.Optional;

/**
 * Single route engine factory.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class SingleRouteEngineFactory {
    
    /**
     * Create new instance of single route engine.
     *
     * @param singleTables single tables
     * @param sqlStatement SQL statement
     * @param hintValueContext hint value context
     * @return created instance
     */
    public static Optional<SingleRouteEngine> newInstance(final Collection<QualifiedTable> singleTables, final SQLStatement sqlStatement, final HintValueContext hintValueContext) {
        if (!singleTables.isEmpty()) {
            return Optional.of(new SingleStandardRouteEngine(singleTables, sqlStatement, hintValueContext));
        }
        // TODO move this logic to common route logic
        if (isSchemaDDLStatement(sqlStatement)) {
            return Optional.of(new SingleDatabaseBroadcastRouteEngine());
        }
        return Optional.empty();
    }
    
    private static boolean isSchemaDDLStatement(final SQLStatement sqlStatement) {
        return sqlStatement instanceof CreateSchemaStatement || sqlStatement instanceof AlterSchemaStatement || sqlStatement instanceof DropSchemaStatement;
    }
}
