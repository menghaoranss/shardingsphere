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

package org.apache.shardingsphere.readwritesplitting.distsql.handler.converter;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.apache.shardingsphere.infra.algorithm.core.config.AlgorithmConfiguration;
import org.apache.shardingsphere.readwritesplitting.config.ReadwriteSplittingRuleConfiguration;
import org.apache.shardingsphere.readwritesplitting.config.rule.ReadwriteSplittingDataSourceGroupRuleConfiguration;
import org.apache.shardingsphere.readwritesplitting.transaction.TransactionalReadQueryStrategy;
import org.apache.shardingsphere.readwritesplitting.distsql.segment.ReadwriteSplittingRuleSegment;

import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

/**
 * Readwrite-splitting rule statement converter.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class ReadwriteSplittingRuleStatementConverter {
    
    /**
     * Convert readwrite-splitting rule segments to readwrite-splitting rule configuration.
     *
     * @param ruleSegments readwrite-splitting rule segments
     * @return readwrite-splitting rule configuration
     */
    public static ReadwriteSplittingRuleConfiguration convert(final Collection<ReadwriteSplittingRuleSegment> ruleSegments) {
        Collection<ReadwriteSplittingDataSourceGroupRuleConfiguration> dataSourceGroups = new LinkedList<>();
        Map<String, AlgorithmConfiguration> loadBalancers = new HashMap<>(ruleSegments.size(), 1F);
        for (ReadwriteSplittingRuleSegment each : ruleSegments) {
            if (null == each.getLoadBalancer()) {
                dataSourceGroups.add(createDataSourceGroupRuleConfiguration(each, null));
            } else {
                String loadBalancerName = getLoadBalancerName(each.getName(), each.getLoadBalancer().getName());
                loadBalancers.put(loadBalancerName, createLoadBalancer(each));
                dataSourceGroups.add(createDataSourceGroupRuleConfiguration(each, loadBalancerName));
            }
        }
        return new ReadwriteSplittingRuleConfiguration(dataSourceGroups, loadBalancers);
    }
    
    private static ReadwriteSplittingDataSourceGroupRuleConfiguration createDataSourceGroupRuleConfiguration(final ReadwriteSplittingRuleSegment segment, final String loadBalancerName) {
        return null == segment.getTransactionalReadQueryStrategy()
                ? new ReadwriteSplittingDataSourceGroupRuleConfiguration(segment.getName(), segment.getWriteDataSource(), new LinkedList<>(segment.getReadDataSources()), loadBalancerName)
                : new ReadwriteSplittingDataSourceGroupRuleConfiguration(segment.getName(), segment.getWriteDataSource(), new LinkedList<>(segment.getReadDataSources()),
                        TransactionalReadQueryStrategy.valueOf(segment.getTransactionalReadQueryStrategy().toUpperCase()), loadBalancerName);
    }
    
    private static AlgorithmConfiguration createLoadBalancer(final ReadwriteSplittingRuleSegment ruleSegment) {
        return new AlgorithmConfiguration(ruleSegment.getLoadBalancer().getName(), ruleSegment.getLoadBalancer().getProps());
    }
    
    private static String getLoadBalancerName(final String ruleName, final String type) {
        return String.format("%s_%s", ruleName, type);
    }
}
