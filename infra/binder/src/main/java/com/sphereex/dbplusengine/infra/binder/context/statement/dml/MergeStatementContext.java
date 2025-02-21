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

package com.sphereex.dbplusengine.infra.binder.context.statement.dml;

import lombok.Getter;
import org.apache.shardingsphere.infra.binder.context.aware.ParameterAware;
import org.apache.shardingsphere.infra.binder.context.segment.table.TablesContext;
import org.apache.shardingsphere.infra.binder.context.statement.CommonSQLStatementContext;
import org.apache.shardingsphere.infra.binder.context.statement.SQLStatementContextFactory;
import org.apache.shardingsphere.infra.binder.context.statement.dml.InsertStatementContext;
import org.apache.shardingsphere.infra.binder.context.statement.dml.SelectStatementContext;
import org.apache.shardingsphere.infra.binder.context.statement.dml.UpdateStatementContext;
import org.apache.shardingsphere.infra.binder.context.type.TableAvailable;
import org.apache.shardingsphere.infra.metadata.ShardingSphereMetaData;
import org.apache.shardingsphere.sql.parser.statement.core.segment.dml.expr.simple.ParameterMarkerExpressionSegment;
import org.apache.shardingsphere.sql.parser.statement.core.segment.generic.ParameterMarkerSegment;
import org.apache.shardingsphere.sql.parser.statement.core.segment.generic.table.SimpleTableSegment;
import org.apache.shardingsphere.sql.parser.statement.core.segment.generic.table.SubqueryTableSegment;
import org.apache.shardingsphere.sql.parser.statement.core.statement.dml.MergeStatement;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;

/**
 * Merge statement context.
 */
@Getter
public final class MergeStatementContext extends CommonSQLStatementContext implements TableAvailable, ParameterAware {
    
    private final SelectStatementContext sourceSubQueryStatement;
    
    private final InsertStatementContext insertStatementContext;
    
    private final UpdateStatementContext updateStatementContext;
    
    private final TablesContext tablesContext;
    
    public MergeStatementContext(final MergeStatement sqlStatement, final List<Object> params, final ShardingSphereMetaData metaData, final String currentDatabaseName) {
        super(sqlStatement);
        if (sqlStatement.getSource() instanceof SubqueryTableSegment) {
            sourceSubQueryStatement = (SelectStatementContext) SQLStatementContextFactory.newInstance(metaData, ((SubqueryTableSegment) sqlStatement.getSource()).getSubquery().getSelect(),
                    getParameters(params, ((SubqueryTableSegment) sqlStatement.getSource()).getSubquery().getSelect().getParameterMarkerSegments()), currentDatabaseName);
        } else {
            sourceSubQueryStatement = null;
        }
        if (sqlStatement.getInsert().isPresent()) {
            sqlStatement.getInsert().get().setTable((SimpleTableSegment) sqlStatement.getTarget());
            insertStatementContext = (InsertStatementContext) SQLStatementContextFactory.newInstance(metaData,
                    sqlStatement.getInsert().get(), getParameters(params, sqlStatement.getInsert().get().getParameterMarkerSegments()), currentDatabaseName);
        } else {
            insertStatementContext = null;
        }
        if (sqlStatement.getUpdate().isPresent()) {
            sqlStatement.getUpdate().get().setTable(sqlStatement.getTarget());
            updateStatementContext = (UpdateStatementContext) SQLStatementContextFactory.newInstance(metaData,
                    sqlStatement.getUpdate().get(), getParameters(params, sqlStatement.getUpdate().get().getParameterMarkerSegments()), currentDatabaseName);
        } else {
            updateStatementContext = null;
        }
        tablesContext = new TablesContext(getSimpleTableSegments(sqlStatement));
    }
    
    private Collection<SimpleTableSegment> getSimpleTableSegments(final MergeStatement sqlStatement) {
        Collection<SimpleTableSegment> result = new LinkedList<>();
        if (sqlStatement.getSource() instanceof SubqueryTableSegment) {
            result.addAll(sourceSubQueryStatement.getTablesContext().getSimpleTables());
        }
        if (sqlStatement.getSource() instanceof SimpleTableSegment) {
            result.add((SimpleTableSegment) sqlStatement.getSource());
        }
        if (sqlStatement.getTarget() instanceof SimpleTableSegment) {
            result.add((SimpleTableSegment) sqlStatement.getTarget());
        }
        return result;
    }
    
    private List<Object> getParameters(final List<Object> params, final Collection<ParameterMarkerSegment> parameterMarkerSegments) {
        if (params.isEmpty()) {
            return Collections.emptyList();
        }
        List<Object> result = new LinkedList<>();
        for (ParameterMarkerSegment each : parameterMarkerSegments) {
            result.add(params.get(((ParameterMarkerExpressionSegment) each).getParameterMarkerIndex()));
        }
        return result;
    }
    
    /**
     * Get source subquery statement.
     *
     * @return source subquery statement
     */
    public Optional<SelectStatementContext> getSourceSubQueryStatement() {
        return Optional.ofNullable(sourceSubQueryStatement);
    }
    
    @Override
    public MergeStatement getSqlStatement() {
        return (MergeStatement) super.getSqlStatement();
    }
    
    @Override
    public void setUpParameters(final List<Object> params) {
        if (null != insertStatementContext) {
            getSqlStatement().getInsert().ifPresent(optional -> insertStatementContext.setUpParameters(getParameters(params, optional.getParameterMarkerSegments())));
        }
    }
}
