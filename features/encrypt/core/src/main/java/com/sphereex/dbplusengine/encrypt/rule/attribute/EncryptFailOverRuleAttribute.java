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

package com.sphereex.dbplusengine.encrypt.rule.attribute;

import com.sphereex.dbplusengine.infra.rule.attribute.failover.FailOverRuleAttribute;
import lombok.RequiredArgsConstructor;
import org.apache.shardingsphere.encrypt.rule.EncryptRule;
import org.apache.shardingsphere.encrypt.rule.table.EncryptTable;
import org.apache.shardingsphere.infra.metadata.database.resource.ResourceMetaData;
import org.apache.shardingsphere.sql.parser.statement.core.extractor.TableExtractor;
import org.apache.shardingsphere.sql.parser.statement.core.segment.generic.table.SimpleTableSegment;
import org.apache.shardingsphere.sql.parser.statement.core.statement.SQLStatement;
import org.apache.shardingsphere.sql.parser.statement.core.statement.dml.SelectStatement;

import java.util.Collection;
import java.util.Optional;

/**
 * Encrypt fail over rule attribute.
 */
@RequiredArgsConstructor
public final class EncryptFailOverRuleAttribute implements FailOverRuleAttribute {
    
    private final String databaseName;
    
    private final EncryptRule encryptRule;
    
    @Override
    public boolean isNeedFailOver(final SQLStatement sqlStatement) {
        if (!encryptRule.getEncryptMode().isUseOriginalSQLWhenCipherQueryFailed() || !(sqlStatement instanceof SelectStatement)) {
            return false;
        }
        for (SimpleTableSegment each : getAllTableSegments((SelectStatement) sqlStatement)) {
            Optional<EncryptTable> encryptTable = encryptRule.findEncryptTable(each.getTableName().getIdentifier().getValue());
            if (encryptTable.isPresent() && !encryptRule.getEncryptTable(each.getTableName().getIdentifier().getValue()).isAllCipherColumnConfigPlain()) {
                return false;
            }
        }
        return true;
    }
    
    private Collection<SimpleTableSegment> getAllTableSegments(final SelectStatement selectStatement) {
        TableExtractor tableExtractor = new TableExtractor();
        tableExtractor.extractTablesFromSelect(selectStatement);
        return tableExtractor.getRewriteTables();
    }
    
    @Override
    public String getFailOverDataSourceName(final ResourceMetaData resourceMetaData) {
        // TODO consider optimize this logic when multi FailOverRuleAttribute use together
        return resourceMetaData.getDataSourceMap().keySet().iterator().next();
    }
}
