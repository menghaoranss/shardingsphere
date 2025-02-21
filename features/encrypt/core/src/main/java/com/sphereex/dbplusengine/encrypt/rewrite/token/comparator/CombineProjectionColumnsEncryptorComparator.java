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

package com.sphereex.dbplusengine.encrypt.rewrite.token.comparator;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.apache.shardingsphere.encrypt.rewrite.token.comparator.EncryptorComparator;
import org.apache.shardingsphere.encrypt.rule.EncryptRule;
import org.apache.shardingsphere.infra.binder.context.segment.select.projection.Projection;
import org.apache.shardingsphere.infra.binder.context.segment.select.projection.impl.ColumnProjection;
import org.apache.shardingsphere.infra.binder.context.statement.dml.SelectStatementContext;
import org.apache.shardingsphere.infra.exception.core.ShardingSpherePreconditions;
import org.apache.shardingsphere.infra.exception.generic.UnsupportedSQLOperationException;
import org.apache.shardingsphere.sql.parser.statement.core.segment.dml.combine.CombineSegment;
import org.apache.shardingsphere.sql.parser.statement.core.segment.generic.bound.ColumnSegmentBoundInfo;
import org.apache.shardingsphere.sql.parser.statement.core.segment.generic.bound.TableSegmentBoundInfo;
import org.apache.shardingsphere.sql.parser.statement.core.value.identifier.IdentifierValue;

import java.util.List;
import java.util.Map;

/**
 * Combine projection columns encryptor comparator.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class CombineProjectionColumnsEncryptorComparator {
    
    /**
     * Judge whether all combine projection columns use same encryptor.
     *
     * @param selectStatementContext select statement context
     * @param rule encrypt rule
     * @param databaseEncryptRules database and encrypt rule map
     * @return using same or different encryptors
     */
    public static boolean isSame(final SelectStatementContext selectStatementContext, final EncryptRule rule, final Map<String, EncryptRule> databaseEncryptRules) {
        if (!selectStatementContext.getSqlStatement().getCombine().isPresent()) {
            return true;
        }
        CombineSegment combineSegment = selectStatementContext.getSqlStatement().getCombine().get();
        List<Projection> leftProjections = selectStatementContext.getSubqueryContexts().get(combineSegment.getLeft().getStartIndex()).getProjectionsContext().getExpandProjections();
        List<Projection> rightProjections = selectStatementContext.getSubqueryContexts().get(combineSegment.getRight().getStartIndex()).getProjectionsContext().getExpandProjections();
        ShardingSpherePreconditions.checkState(leftProjections.size() == rightProjections.size(), () -> new UnsupportedSQLOperationException("Column projections must be same for combine statement"));
        for (int i = 0; i < leftProjections.size(); i++) {
            ColumnSegmentBoundInfo leftColumnInfo = getColumnSegmentBoundInfo(leftProjections.get(i));
            ColumnSegmentBoundInfo rightColumnInfo = getColumnSegmentBoundInfo(rightProjections.get(i));
            if (!EncryptorComparator.isSame(rule, leftColumnInfo, rightColumnInfo, databaseEncryptRules)) {
                return false;
            }
        }
        return true;
    }
    
    private static ColumnSegmentBoundInfo getColumnSegmentBoundInfo(final Projection projection) {
        return projection instanceof ColumnProjection
                ? new ColumnSegmentBoundInfo(
                        new TableSegmentBoundInfo(((ColumnProjection) projection).getColumnBoundInfo().getOriginalDatabase(), ((ColumnProjection) projection).getColumnBoundInfo().getOriginalSchema()),
                        ((ColumnProjection) projection).getOriginalTable(), ((ColumnProjection) projection).getOriginalColumn())
                : new ColumnSegmentBoundInfo(new IdentifierValue(projection.getColumnLabel()));
    }
}
