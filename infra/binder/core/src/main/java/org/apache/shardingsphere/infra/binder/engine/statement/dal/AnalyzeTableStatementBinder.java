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

package org.apache.shardingsphere.infra.binder.engine.statement.dal;

import com.cedarsoftware.util.CaseInsensitiveMap.CaseInsensitiveString;
import com.google.common.collect.LinkedHashMultimap;
import com.google.common.collect.Multimap;
import org.apache.shardingsphere.infra.binder.engine.segment.dml.from.context.TableSegmentBinderContext;
import org.apache.shardingsphere.infra.binder.engine.segment.dml.from.type.SimpleTableSegmentBinder;
import org.apache.shardingsphere.infra.binder.engine.statement.SQLStatementBinder;
import org.apache.shardingsphere.infra.binder.engine.statement.SQLStatementBinderContext;
import org.apache.shardingsphere.infra.binder.engine.statement.SQLStatementCopyUtils;
import org.apache.shardingsphere.sql.parser.statement.core.segment.generic.table.SimpleTableSegment;
import org.apache.shardingsphere.sql.parser.statement.core.statement.type.dal.AnalyzeTableStatement;

import java.util.Collection;
import java.util.stream.Collectors;

/**
 * Analyze table statement binder.
 */
public final class AnalyzeTableStatementBinder implements SQLStatementBinder<AnalyzeTableStatement> {
    
    @Override
    public AnalyzeTableStatement bind(final AnalyzeTableStatement sqlStatement, final SQLStatementBinderContext binderContext) {
        Multimap<CaseInsensitiveString, TableSegmentBinderContext> tableBinderContexts = LinkedHashMultimap.create();
        Collection<SimpleTableSegment> boundTables = sqlStatement.getTables()
                .stream()
                .map(each -> SimpleTableSegmentBinder.bind(each, binderContext, tableBinderContexts))
                .collect(Collectors.toList());
        return copy(sqlStatement, boundTables);
    }
    
    private AnalyzeTableStatement copy(final AnalyzeTableStatement sqlStatement, final Collection<SimpleTableSegment> tables) {
        AnalyzeTableStatement result = new AnalyzeTableStatement(sqlStatement.getDatabaseType(), tables);
        SQLStatementCopyUtils.copyAttributes(sqlStatement, result);
        return result;
    }
}
