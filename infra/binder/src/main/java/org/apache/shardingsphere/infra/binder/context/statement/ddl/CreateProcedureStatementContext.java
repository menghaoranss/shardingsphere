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

package org.apache.shardingsphere.infra.binder.context.statement.ddl;

import com.sphereex.dbplusengine.SphereEx;
import lombok.Getter;
import org.apache.shardingsphere.infra.binder.context.segment.table.TablesContext;
import org.apache.shardingsphere.infra.binder.context.statement.CommonSQLStatementContext;
import org.apache.shardingsphere.infra.binder.context.statement.SQLStatementContext;
import org.apache.shardingsphere.infra.binder.context.statement.SQLStatementContextFactory;
import org.apache.shardingsphere.infra.metadata.ShardingSphereMetaData;
import org.apache.shardingsphere.sql.parser.statement.core.extractor.TableExtractor;
import org.apache.shardingsphere.sql.parser.statement.core.segment.ddl.routine.RoutineBodySegment;
import org.apache.shardingsphere.sql.parser.statement.core.segment.generic.table.SimpleTableSegment;
import org.apache.shardingsphere.sql.parser.statement.core.statement.ddl.CreateProcedureStatement;
import org.apache.shardingsphere.sql.parser.statement.oracle.ddl.OracleCreateProcedureStatement;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.Optional;

/**
 * Create procedure statement context.
 */
@Getter
public final class CreateProcedureStatementContext extends CommonSQLStatementContext {
    
    private final TablesContext tablesContext;
    
    @SphereEx
    private final Collection<SQLStatementContext> sqlStatementContexts = new LinkedList<>();
    
    public CreateProcedureStatementContext(final CreateProcedureStatement sqlStatement, @SphereEx final String currentDatabaseName, @SphereEx final ShardingSphereMetaData metaData) {
        super(sqlStatement);
        Optional<RoutineBodySegment> routineBodySegment = sqlStatement.getRoutineBody();
        Collection<SimpleTableSegment> tables = routineBodySegment.map(optional -> new TableExtractor().extractExistTableFromRoutineBody(optional)).orElseGet(Collections::emptyList);
        tablesContext = new TablesContext(tables);
        // SPEX ADDED: BEGIN
        if (sqlStatement instanceof OracleCreateProcedureStatement) {
            ((OracleCreateProcedureStatement) sqlStatement).getSqlStatements()
                    .forEach(each -> sqlStatementContexts.add(SQLStatementContextFactory.newInstance(metaData, each.getSqlStatement(), Collections.emptyList(), currentDatabaseName)));
        }
        // SPEX ADDED: END
    }
    
    @Override
    public CreateProcedureStatement getSqlStatement() {
        return (CreateProcedureStatement) super.getSqlStatement();
    }
}
