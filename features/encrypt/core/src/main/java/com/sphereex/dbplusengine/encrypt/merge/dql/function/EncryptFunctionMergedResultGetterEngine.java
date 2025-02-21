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

package com.sphereex.dbplusengine.encrypt.merge.dql.function;

import com.cedarsoftware.util.CaseInsensitiveSet;
import lombok.RequiredArgsConstructor;
import org.apache.shardingsphere.encrypt.rule.EncryptRule;
import org.apache.shardingsphere.infra.binder.context.segment.select.projection.Projection;
import org.apache.shardingsphere.infra.binder.context.segment.select.projection.impl.AggregationProjection;
import org.apache.shardingsphere.infra.binder.context.segment.select.projection.impl.ExpressionProjection;
import org.apache.shardingsphere.infra.binder.context.statement.dml.SelectStatementContext;
import org.apache.shardingsphere.infra.database.core.type.DatabaseType;
import org.apache.shardingsphere.infra.merge.result.MergedResult;
import org.apache.shardingsphere.infra.metadata.database.ShardingSphereDatabase;
import org.apache.shardingsphere.infra.spi.type.typed.TypedSPILoader;
import org.apache.shardingsphere.sql.parser.statement.core.segment.dml.expr.FunctionSegment;
import org.apache.shardingsphere.sql.parser.statement.core.segment.dml.item.AggregationProjectionSegment;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Optional;

/**
 * Encrypt function merged result getter engine.
 */
@RequiredArgsConstructor
public final class EncryptFunctionMergedResultGetterEngine {
    
    private static final Collection<String> SUPPORTED_AGGREGATION_FUNCTIONS = new CaseInsensitiveSet<>(Arrays.asList("MAX", "MIN"));
    
    private final ShardingSphereDatabase database;
    
    private final SelectStatementContext selectStatementContext;
    
    private final MergedResult mergedResult;
    
    /**
     * Get value.
     *
     * @param columnIndex column index
     * @param type class type of data value
     * @return data value
     * @throws SQLException SQL exception
     */
    public Object getValue(final int columnIndex, final Class<?> type) throws SQLException {
        List<Projection> expandProjections = selectStatementContext.getProjectionsContext().getExpandProjections();
        // NOTE: 防篡改 + 加密场景, 会将 select name 改写成 select name_cipher, name_digest 两列,获取 name_digest 值时, 列长度会超出 expandProjections 大小
        if (columnIndex >= expandProjections.size() + 1) {
            return mergedResult.getValue(columnIndex, type);
        }
        Projection projection = expandProjections.get(columnIndex - 1);
        DatabaseType databaseType = selectStatementContext.getDatabaseType();
        // TODO support cross database encrypt @zhangcheng
        if (projection instanceof ExpressionProjection && ((ExpressionProjection) projection).getExpressionSegment().getExpr() instanceof FunctionSegment) {
            FunctionSegment functionSegment = (FunctionSegment) ((ExpressionProjection) projection).getExpressionSegment().getExpr();
            Optional<EncryptFunctionMergedResultGetter> functionMergedResultGetter = findFunctionMergedResultGetter(functionSegment.getFunctionName(), databaseType);
            if (functionMergedResultGetter.isPresent() && functionMergedResultGetter.get().isNeedDecrypt(functionSegment)) {
                return functionMergedResultGetter.get().getValue(mergedResult, database.getRuleMetaData().getSingleRule(EncryptRule.class), databaseType, functionSegment, columnIndex, type);
            }
        }
        if (projection instanceof AggregationProjection && SUPPORTED_AGGREGATION_FUNCTIONS.contains(((AggregationProjection) projection).getType().name())) {
            AggregationProjectionSegment aggregationSegment = ((AggregationProjection) projection).getAggregationSegment();
            Optional<EncryptFunctionMergedResultGetter> functionMergedResultGetter = findFunctionMergedResultGetter(aggregationSegment.getType().name(), databaseType);
            if (functionMergedResultGetter.isPresent() && functionMergedResultGetter.get().isNeedDecrypt(aggregationSegment)) {
                return functionMergedResultGetter.get().getValue(mergedResult, database.getRuleMetaData().getSingleRule(EncryptRule.class), databaseType, aggregationSegment, columnIndex, type);
            }
        }
        return mergedResult.getValue(columnIndex, type);
    }
    
    private Optional<EncryptFunctionMergedResultGetter> findFunctionMergedResultGetter(final String functionName, final DatabaseType databaseType) {
        Optional<EncryptFunctionMergedResultGetter> result = Optional.ofNullable(TypedSPILoader.findService(EncryptFunctionMergedResultGetter.class, functionName)
                .orElseGet(() -> TypedSPILoader.findService(EncryptFunctionMergedResultGetter.class, databaseType.getType() + ":" + functionName).orElse(null)));
        if (!result.isPresent() && databaseType.getTrunkDatabaseType().isPresent()) {
            return TypedSPILoader.findService(EncryptFunctionMergedResultGetter.class, databaseType.getTrunkDatabaseType().get().getType() + ":" + functionName);
        }
        return result;
    }
}
