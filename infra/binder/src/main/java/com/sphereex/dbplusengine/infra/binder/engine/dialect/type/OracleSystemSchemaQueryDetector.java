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

package com.sphereex.dbplusengine.infra.binder.engine.dialect.type;

import com.sphereex.dbplusengine.infra.binder.engine.dialect.SystemSchemaQueryDetector;
import org.apache.shardingsphere.infra.metadata.database.schema.manager.SystemSchemaManager;
import org.apache.shardingsphere.sql.parser.statement.core.extractor.TableExtractor;
import org.apache.shardingsphere.sql.parser.statement.core.segment.generic.OwnerSegment;
import org.apache.shardingsphere.sql.parser.statement.core.segment.generic.table.SimpleTableSegment;
import org.apache.shardingsphere.sql.parser.statement.core.statement.SQLStatement;
import org.apache.shardingsphere.sql.parser.statement.core.statement.dml.SelectStatement;
import org.apache.shardingsphere.sql.parser.statement.core.value.identifier.IdentifierValue;

/**
 * Oracle system schema query detector.
 */
public final class OracleSystemSchemaQueryDetector implements SystemSchemaQueryDetector {
    
    @Override
    public boolean isSystemSchemaQuery(final SQLStatement statement) {
        if (!(statement instanceof SelectStatement) || !((SelectStatement) statement).getFrom().isPresent()) {
            return false;
        }
        TableExtractor tableExtractor = new TableExtractor();
        tableExtractor.extractTablesFromSQLStatement(statement);
        for (SimpleTableSegment each : tableExtractor.getRewriteTables()) {
            if (each.getOwner().map(OwnerSegment::getIdentifier).map(IdentifierValue::getValue).map("PUBLIC"::equalsIgnoreCase).orElse(false)) {
                return true;
            }
            if (SystemSchemaManager.isSystemTable("oracle", "public", each.getTableName().getIdentifier().getValue())) {
                return true;
            }
        }
        return false;
    }
    
    @Override
    public String getDatabaseType() {
        return "Oracle";
    }
}
