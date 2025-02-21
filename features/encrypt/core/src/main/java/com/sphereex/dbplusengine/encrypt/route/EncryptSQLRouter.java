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

package com.sphereex.dbplusengine.encrypt.route;

import com.sphereex.dbplusengine.encrypt.rule.attribute.EncryptDataNodeRuleAttribute;
import com.sphereex.dbplusengine.encrypt.config.rule.mode.EncryptModeType;
import org.apache.shardingsphere.encrypt.constant.EncryptOrder;
import org.apache.shardingsphere.encrypt.rule.EncryptRule;
import org.apache.shardingsphere.encrypt.rule.table.EncryptTable;
import org.apache.shardingsphere.infra.annotation.HighFrequencyInvocation;
import org.apache.shardingsphere.infra.config.props.ConfigurationProperties;
import org.apache.shardingsphere.infra.datanode.DataNode;
import org.apache.shardingsphere.infra.metadata.database.ShardingSphereDatabase;
import org.apache.shardingsphere.infra.metadata.database.rule.RuleMetaData;
import org.apache.shardingsphere.infra.route.context.RouteContext;
import org.apache.shardingsphere.infra.route.context.RouteMapper;
import org.apache.shardingsphere.infra.route.context.RouteUnit;
import org.apache.shardingsphere.infra.route.lifecycle.DecorateSQLRouter;
import org.apache.shardingsphere.infra.route.lifecycle.EntranceSQLRouter;
import org.apache.shardingsphere.infra.session.query.QueryContext;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.Optional;

/**
 * Encrypt SQL router.
 */
@HighFrequencyInvocation
public final class EncryptSQLRouter implements EntranceSQLRouter<EncryptRule>, DecorateSQLRouter<EncryptRule> {
    
    @Override
    public RouteContext createRouteContext(final QueryContext queryContext, final RuleMetaData globalRuleMetaData, final ShardingSphereDatabase database, final EncryptRule rule,
                                           final Collection<String> tableNames, final ConfigurationProperties props) {
        RouteContext result = new RouteContext();
        if (EncryptModeType.FRONTEND == rule.getEncryptMode().getType()) {
            return result;
        }
        result.getRouteUnits().addAll(createRouteUnits(tableNames, rule));
        return result;
    }
    
    private Collection<RouteUnit> createRouteUnits(final Collection<String> tableNames, final EncryptRule rule) {
        Collection<RouteUnit> result = new LinkedList<>();
        for (String each : tableNames) {
            if (!rule.findEncryptTable(each).isPresent()) {
                continue;
            }
            Collection<DataNode> dataNodes = rule.getAttributes().getAttribute(EncryptDataNodeRuleAttribute.class).getDataNodesByTableName(each);
            if (!dataNodes.isEmpty()) {
                DataNode dataNode = dataNodes.iterator().next();
                result.add(new RouteUnit(new RouteMapper(dataNode.getDataSourceName(), dataNode.getDataSourceName()), Collections.singleton(new RouteMapper(each, dataNode.getTableName()))));
            }
        }
        return result;
    }
    
    @Override
    public void decorateRouteContext(final RouteContext routeContext, final QueryContext queryContext, final ShardingSphereDatabase database,
                                     final EncryptRule rule, final Collection<String> tableNames, final ConfigurationProperties props) {
        if (EncryptModeType.FRONTEND == rule.getEncryptMode().getType()) {
            return;
        }
        for (RouteUnit each : routeContext.getRouteUnits()) {
            decorateRouteMapper(each, rule);
        }
    }
    
    private void decorateRouteMapper(final RouteUnit routeUnit, final EncryptRule rule) {
        Collection<RouteMapper> toBeRemoved = new LinkedList<>();
        Collection<RouteMapper> toBeAdded = new LinkedList<>();
        for (RouteMapper each : routeUnit.getTableMappers()) {
            Optional<EncryptTable> encryptTable = rule.findEncryptTable(each.getActualName());
            if (!encryptTable.isPresent() || !encryptTable.get().getRenameTable().isPresent()) {
                continue;
            }
            toBeRemoved.add(each);
            toBeAdded.add(new RouteMapper(each.getLogicName(), encryptTable.get().getRenameTable().get()));
        }
        routeUnit.getTableMappers().removeAll(toBeRemoved);
        routeUnit.getTableMappers().addAll(toBeAdded);
    }
    
    @Override
    public Type getType() {
        return Type.DATA_NODE;
    }
    
    @Override
    public int getOrder() {
        return EncryptOrder.ORDER;
    }
    
    @Override
    public Class<EncryptRule> getTypeClass() {
        return EncryptRule.class;
    }
}
