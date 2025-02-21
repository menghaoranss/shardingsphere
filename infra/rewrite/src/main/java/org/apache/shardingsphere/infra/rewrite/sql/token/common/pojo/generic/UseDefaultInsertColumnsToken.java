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

package org.apache.shardingsphere.infra.rewrite.sql.token.common.pojo.generic;

import com.sphereex.dbplusengine.SphereEx;
import lombok.Getter;
import org.apache.shardingsphere.infra.database.core.metadata.database.enums.QuoteCharacter;
import org.apache.shardingsphere.infra.rewrite.sql.token.common.pojo.Attachable;
import org.apache.shardingsphere.infra.rewrite.sql.token.common.pojo.SQLToken;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Default insert columns token.
 */
@Getter
public final class UseDefaultInsertColumnsToken extends SQLToken implements Attachable {
    
    private final List<String> columns;
    
    private final QuoteCharacter quoteCharacter;
    
    @SphereEx
    @Getter
    private final Map<Integer, Collection<String>> addedColumns = new HashMap<>();
    
    public UseDefaultInsertColumnsToken(final int startIndex, final List<String> columns) {
        super(startIndex);
        this.columns = columns;
        this.quoteCharacter = QuoteCharacter.NONE;
    }
    
    public UseDefaultInsertColumnsToken(final int startIndex, final List<String> columns, final QuoteCharacter quoteCharacter) {
        super(startIndex);
        this.columns = columns;
        this.quoteCharacter = quoteCharacter;
    }
    
    /**
     * Add added column name.
     *
     * @param index index
     * @param columnName column name
     */
    @SphereEx
    public void addAddedValue(final int index, final String columnName) {
        addedColumns.computeIfAbsent(index, unused -> new LinkedList<>()).add(columnName);
    }
    
    @Override
    public String toString() {
        return columns.isEmpty() ? "" : "(" + String.join(", ", getColumnNames()) + ")";
    }
    
    private Collection<String> getColumnNames() {
        Collection<String> result = new ArrayList<>(columns.size());
        // SPEX ADDED: BEGIN
        int index = 0;
        // SPEX ADDED: END
        for (String each : columns) {
            result.add(quoteCharacter.wrap(each));
            // SPEX ADDED: BEGIN
            Collection<String> currentAddedColumns = addedColumns.get(index++);
            if (null == currentAddedColumns) {
                continue;
            }
            for (String column : currentAddedColumns) {
                result.add(quoteCharacter.wrap(column));
            }
        }
        // SPEX ADDED: END
        return result;
    }
    
    @Override
    public int getStopIndex() {
        return getStartIndex();
    }
}
