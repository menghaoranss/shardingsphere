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

package com.sphereex.dbplusengine.infra.binder.engine.statement.ddl;

import com.cedarsoftware.util.CaseInsensitiveMap.CaseInsensitiveString;
import com.google.common.base.Strings;
import com.google.common.collect.LinkedHashMultimap;
import com.google.common.collect.Multimap;
import com.sphereex.dbplusengine.infra.binder.engine.statement.dml.MergeStatementBinder;
import org.apache.shardingsphere.infra.binder.engine.segment.dml.from.context.TableSegmentBinderContext;
import org.apache.shardingsphere.infra.binder.engine.segment.dml.from.context.type.SimpleTableSegmentBinderContext;
import org.apache.shardingsphere.infra.binder.engine.statement.SQLStatementBinder;
import org.apache.shardingsphere.infra.binder.engine.statement.SQLStatementBinderContext;
import org.apache.shardingsphere.infra.binder.engine.statement.ddl.CursorStatementBinder;
import org.apache.shardingsphere.infra.binder.engine.statement.dml.DeleteStatementBinder;
import org.apache.shardingsphere.infra.binder.engine.statement.dml.InsertStatementBinder;
import org.apache.shardingsphere.infra.binder.engine.statement.dml.SelectStatementBinder;
import org.apache.shardingsphere.infra.binder.engine.statement.dml.UpdateStatementBinder;
import org.apache.shardingsphere.sql.parser.statement.core.segment.dml.column.ColumnSegment;
import org.apache.shardingsphere.sql.parser.statement.core.segment.dml.item.ColumnProjectionSegment;
import org.apache.shardingsphere.sql.parser.statement.core.segment.dml.item.ProjectionSegment;
import org.apache.shardingsphere.sql.parser.statement.core.segment.dml.item.ShorthandProjectionSegment;
import org.apache.shardingsphere.sql.parser.statement.core.segment.generic.OwnerSegment;
import org.apache.shardingsphere.sql.parser.statement.core.segment.generic.bound.ColumnSegmentBoundInfo;
import org.apache.shardingsphere.sql.parser.statement.core.segment.generic.bound.TableSegmentBoundInfo;
import org.apache.shardingsphere.sql.parser.statement.core.statement.SQLStatement;
import org.apache.shardingsphere.sql.parser.statement.core.statement.ddl.DDLStatement;
import org.apache.shardingsphere.sql.parser.statement.core.statement.dml.DMLStatement;
import org.apache.shardingsphere.sql.parser.statement.core.statement.dml.DeleteStatement;
import org.apache.shardingsphere.sql.parser.statement.core.statement.dml.InsertStatement;
import org.apache.shardingsphere.sql.parser.statement.core.statement.dml.MergeStatement;
import org.apache.shardingsphere.sql.parser.statement.core.statement.dml.SelectStatement;
import org.apache.shardingsphere.sql.parser.statement.core.statement.dml.UpdateStatement;
import org.apache.shardingsphere.sql.parser.statement.core.value.identifier.IdentifierValue;
import org.apache.shardingsphere.sql.parser.statement.opengauss.ddl.OpenGaussCursorStatement;
import org.apache.shardingsphere.sql.parser.statement.oracle.ddl.OracleCreateProcedureStatement;
import org.apache.shardingsphere.sql.parser.statement.oracle.plsql.CursorForLoopStatementSegment;
import org.apache.shardingsphere.sql.parser.statement.oracle.plsql.SQLStatementSegment;

import java.util.Collection;
import java.util.LinkedList;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;

/**
 * Oracle create procedure statement binder.
 */
public final class OracleCreateProcedureStatementBinder implements SQLStatementBinder<OracleCreateProcedureStatement> {
    
    @Override
    public OracleCreateProcedureStatement bind(final OracleCreateProcedureStatement sqlStatement, final SQLStatementBinderContext binderContext) {
        OracleCreateProcedureStatement result = copy(sqlStatement);
        Map<Collection<SQLStatement>, Multimap<CaseInsensitiveString, TableSegmentBinderContext>> cursorTableBinderContexts =
                sqlStatement.getCursorForLoopStatements().stream().collect(Collectors.toMap(
                        CursorForLoopStatementSegment::getStatements, each -> getTableSegmentBinderContexts(each.getRecord(), (SelectStatement) each.getCursorRelatedStatement(), binderContext)));
        for (SQLStatementSegment each : sqlStatement.getSqlStatements()) {
            binderContext.getExternalTableBinderContexts().putAll(getCursorContexts(each, cursorTableBinderContexts));
            result.getSqlStatements().add(new SQLStatementSegment(each.getStartIndex(), each.getStopIndex(), bind0(each.getSqlStatement(), binderContext)));
        }
        sqlStatement.getProcedureName().ifPresent(result::setProcedureName);
        return result;
    }
    
    private OracleCreateProcedureStatement copy(final OracleCreateProcedureStatement sqlStatement) {
        return new OracleCreateProcedureStatement(
                new LinkedList<>(), sqlStatement.getProcedureCallNames(), sqlStatement.getProcedureBodyEndNameSegments(), sqlStatement.getDynamicSqlStatementExpressions());
    }
    
    private Multimap<CaseInsensitiveString, TableSegmentBinderContext> getTableSegmentBinderContexts(final String cursorRecord, final SelectStatement sqlStatement,
                                                                                                     final SQLStatementBinderContext binderContext) {
        Multimap<CaseInsensitiveString, TableSegmentBinderContext> result = LinkedHashMultimap.create();
        result.put(new CaseInsensitiveString(cursorRecord),
                new SimpleTableSegmentBinderContext(createCursorProjections(new SelectStatementBinder().bind(sqlStatement, binderContext).getProjections().getProjections(), cursorRecord)));
        return result;
    }
    
    private Collection<ProjectionSegment> createCursorProjections(final Collection<ProjectionSegment> projections, final String cursorRecord) {
        Collection<ProjectionSegment> result = new LinkedList<>();
        for (ProjectionSegment each : projections) {
            if (each instanceof ColumnProjectionSegment) {
                result.add(createColumnProjection((ColumnProjectionSegment) each, cursorRecord));
            } else if (each instanceof ShorthandProjectionSegment) {
                result.addAll(createCursorProjections(((ShorthandProjectionSegment) each).getActualProjectionSegments(), cursorRecord));
            } else {
                result.add(each);
            }
        }
        return result;
    }
    
    private ColumnProjectionSegment createColumnProjection(final ColumnProjectionSegment originalColumn, final String cursorRecord) {
        ColumnSegment newColumnSegment = new ColumnSegment(0, 0, originalColumn.getAlias().orElseGet(() -> originalColumn.getColumn().getIdentifier()));
        if (!Strings.isNullOrEmpty(cursorRecord)) {
            newColumnSegment.setOwner(new OwnerSegment(0, 0, new IdentifierValue(cursorRecord)));
        }
        ColumnSegmentBoundInfo columnBoundInfo = originalColumn.getColumn().getColumnBoundInfo();
        newColumnSegment.setColumnBoundInfo(new ColumnSegmentBoundInfo(new TableSegmentBoundInfo(columnBoundInfo.getOriginalDatabase(), columnBoundInfo.getOriginalSchema()),
                columnBoundInfo.getOriginalTable(), columnBoundInfo.getOriginalColumn()));
        ColumnProjectionSegment result = new ColumnProjectionSegment(newColumnSegment);
        result.setVisible(originalColumn.isVisible());
        return result;
    }
    
    private Multimap<CaseInsensitiveString, TableSegmentBinderContext> getCursorContexts(final SQLStatementSegment sqlStatementSegment,
                                                                                         final Map<Collection<SQLStatement>, Multimap<CaseInsensitiveString, TableSegmentBinderContext>> contexts) {
        return contexts.entrySet().stream().filter(entry -> entry.getKey().contains(sqlStatementSegment.getSqlStatement())).findFirst().map(Entry::getValue).orElse(LinkedHashMultimap.create());
    }
    
    private SQLStatement bind0(final SQLStatement statement, final SQLStatementBinderContext binderContext) {
        if (statement instanceof DMLStatement) {
            return bindDMLStatement(statement, binderContext);
        }
        if (statement instanceof DDLStatement) {
            return bindDDLStatement(statement, binderContext);
        }
        return statement;
    }
    
    private SQLStatement bindDMLStatement(final SQLStatement statement, final SQLStatementBinderContext binderContext) {
        if (statement instanceof SelectStatement) {
            return new SelectStatementBinder().bind((SelectStatement) statement, createBinderContext(binderContext, statement));
        }
        if (statement instanceof InsertStatement) {
            return new InsertStatementBinder().bind((InsertStatement) statement, createBinderContext(binderContext, statement));
        }
        if (statement instanceof UpdateStatement) {
            return new UpdateStatementBinder().bind((UpdateStatement) statement, createBinderContext(binderContext, statement));
        }
        if (statement instanceof DeleteStatement) {
            return new DeleteStatementBinder().bind((DeleteStatement) statement, createBinderContext(binderContext, statement));
        }
        if (statement instanceof MergeStatement) {
            return new MergeStatementBinder().bind((MergeStatement) statement, createBinderContext(binderContext, statement));
        }
        return statement;
    }
    
    private SQLStatementBinderContext createBinderContext(final SQLStatementBinderContext binderContext, final SQLStatement statement) {
        SQLStatementBinderContext result = new SQLStatementBinderContext(binderContext.getMetaData(), binderContext.getCurrentDatabaseName(), binderContext.getHintValueContext(), statement);
        result.getExternalTableBinderContexts().putAll(binderContext.getExternalTableBinderContexts());
        result.getPivotColumnNames().addAll(binderContext.getPivotColumnNames());
        return result;
    }
    
    private SQLStatement bindDDLStatement(final SQLStatement statement, final SQLStatementBinderContext binderContext) {
        if (statement instanceof OpenGaussCursorStatement) {
            return new CursorStatementBinder().bind((OpenGaussCursorStatement) statement, binderContext);
        }
        return statement;
    }
}
