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

package org.apache.shardingsphere.sql.parser.statement.core.statement.type.dal;

import org.apache.shardingsphere.infra.database.core.type.DatabaseType;
import lombok.Getter;
import org.apache.shardingsphere.sql.parser.statement.core.segment.generic.table.SimpleTableSegment;
import org.apache.shardingsphere.sql.parser.statement.core.statement.attribute.SQLStatementAttributes;
import org.apache.shardingsphere.sql.parser.statement.core.statement.attribute.type.TableBroadcastRouteSQLStatementAttribute;
import org.apache.shardingsphere.sql.parser.statement.core.statement.attribute.type.TableSQLStatementAttribute;

import java.util.Collection;

/**
 * Analyze table statement.
 */
@Getter
public final class AnalyzeTableStatement extends DALStatement {
    
    private final Collection<SimpleTableSegment> tables;
    
    public AnalyzeTableStatement(final DatabaseType databaseType, final Collection<SimpleTableSegment> tables) {
        super(databaseType);
        this.tables = tables;
    }
    
    @Override
    public SQLStatementAttributes getAttributes() {
        return new SQLStatementAttributes(new TableSQLStatementAttribute(tables), new TableBroadcastRouteSQLStatementAttribute());
    }
}
