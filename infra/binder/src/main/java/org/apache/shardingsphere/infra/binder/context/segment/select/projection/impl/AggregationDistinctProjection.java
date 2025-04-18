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

package org.apache.shardingsphere.infra.binder.context.segment.select.projection.impl;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import org.apache.shardingsphere.infra.database.core.type.DatabaseType;
import org.apache.shardingsphere.sql.parser.statement.core.enums.AggregationType;
import org.apache.shardingsphere.sql.parser.statement.core.segment.dml.item.AggregationProjectionSegment;
import org.apache.shardingsphere.sql.parser.statement.core.value.identifier.IdentifierValue;

/**
 * Aggregation distinct projection.
 */
@EqualsAndHashCode(callSuper = true)
@Getter
public final class AggregationDistinctProjection extends AggregationProjection {
    
    private final int startIndex;
    
    private final int stopIndex;
    
    private final String distinctInnerExpression;
    
    public AggregationDistinctProjection(final int startIndex, final int stopIndex, final AggregationType type, final AggregationProjectionSegment aggregationSegment,
                                         final IdentifierValue alias, final String distinctInnerExpression, final DatabaseType databaseType) {
        super(type, aggregationSegment, alias, databaseType);
        this.startIndex = startIndex;
        this.stopIndex = stopIndex;
        this.distinctInnerExpression = distinctInnerExpression;
    }
    
    public AggregationDistinctProjection(final int startIndex, final int stopIndex, final AggregationType type, final AggregationProjectionSegment aggregationSegment,
                                         final IdentifierValue alias, final String distinctInnerExpression, final DatabaseType databaseType, final String separator) {
        super(type, aggregationSegment, alias, databaseType, separator);
        this.startIndex = startIndex;
        this.stopIndex = stopIndex;
        this.distinctInnerExpression = distinctInnerExpression;
    }
}
