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

import lombok.Getter;
import org.apache.shardingsphere.infra.datanode.DataNode;
import org.apache.shardingsphere.infra.rewrite.sql.token.common.pojo.generic.InsertValue;
import org.apache.shardingsphere.sql.parser.statement.core.segment.dml.expr.ExpressionSegment;
import org.apache.shardingsphere.sql.parser.statement.core.segment.generic.table.TableNameSegment;

import java.util.Collection;
import java.util.List;

/**
 * Multi insert value for sharding.
 */
@Getter
public final class ShardingMultiInsertColumnValue extends InsertValue {
    
    private final TableNameSegment tableName;
    
    private final List<String> columnNames;
    
    private final Collection<DataNode> dataNodes;
    
    public ShardingMultiInsertColumnValue(final List<ExpressionSegment> values, final TableNameSegment tableName, final List<String> columnNames, final Collection<DataNode> dataNodes) {
        super(values);
        this.tableName = tableName;
        this.columnNames = columnNames;
        this.dataNodes = dataNodes;
    }
}
