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

package com.sphereex.dbplusengine.sharding.rewrite.token.pojo;

import com.google.common.base.Joiner;
import org.apache.shardingsphere.infra.binder.context.statement.dml.InsertStatementContext;
import org.apache.shardingsphere.infra.datanode.DataNode;
import org.apache.shardingsphere.infra.rewrite.sql.token.common.pojo.RouteUnitAware;
import org.apache.shardingsphere.infra.rewrite.sql.token.common.pojo.generic.InsertValue;
import org.apache.shardingsphere.infra.rewrite.sql.token.common.pojo.generic.InsertValuesToken;
import org.apache.shardingsphere.infra.route.context.RouteUnit;
import org.apache.shardingsphere.sharding.rewrite.token.pojo.ShardingTokenUtils;
import org.apache.shardingsphere.sharding.rule.ShardingRule;
import org.apache.shardingsphere.sql.parser.statement.core.value.identifier.IdentifierValue;

/**
 * Multi insert column values token for sharding.
 */
public final class ShardingMultiInsertColumnValuesToken extends InsertValuesToken implements RouteUnitAware {
    
    private final InsertStatementContext insertStatementContext;
    
    private final ShardingRule rule;
    
    public ShardingMultiInsertColumnValuesToken(final int startIndex, final int stopIndex, final InsertStatementContext insertStatementContext, final ShardingRule rule) {
        super(startIndex, stopIndex);
        this.insertStatementContext = insertStatementContext;
        this.rule = rule;
    }
    
    @Override
    public String toString(final RouteUnit routeUnit) {
        StringBuilder result = new StringBuilder();
        appendInsertValue(routeUnit, result);
        return result.toString();
    }
    
    @Override
    public String toString() {
        return toString(null);
    }
    
    private void appendInsertValue(final RouteUnit routeUnit, final StringBuilder builder) {
        for (InsertValue each : getInsertValues()) {
            if (!(each instanceof ShardingMultiInsertColumnValue)) {
                continue;
            }
            ShardingMultiInsertColumnValue multiInsertColumnValue = (ShardingMultiInsertColumnValue) each;
            if (isAppend(routeUnit, multiInsertColumnValue)) {
                builder.append(" INTO ").append(getActualTableName(routeUnit, multiInsertColumnValue.getTableName().getIdentifier())).append(" (")
                        .append(Joiner.on(", ").join(multiInsertColumnValue.getColumnNames())).append(") VALUES ").append(each);
            }
        }
    }
    
    private boolean isAppend(final RouteUnit routeUnit, final ShardingMultiInsertColumnValue insertValueToken) {
        if (insertValueToken.getDataNodes().isEmpty() || null == routeUnit) {
            return true;
        }
        for (DataNode each : insertValueToken.getDataNodes()) {
            if (routeUnit.findTableMapper(each.getDataSourceName(), each.getTableName()).isPresent()) {
                return true;
            }
        }
        return false;
    }
    
    private String getActualTableName(final RouteUnit routeUnit, final IdentifierValue tableName) {
        String actualTableName = ShardingTokenUtils.getLogicAndActualTableMap(routeUnit, insertStatementContext, rule).get(tableName.getValue());
        actualTableName = null == actualTableName ? tableName.getValue() : actualTableName;
        return tableName.getQuoteCharacter().wrap(actualTableName);
    }
}
