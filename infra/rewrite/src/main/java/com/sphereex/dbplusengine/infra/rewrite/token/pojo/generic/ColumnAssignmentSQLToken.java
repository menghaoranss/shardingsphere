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

package com.sphereex.dbplusengine.infra.rewrite.token.pojo.generic;

import lombok.RequiredArgsConstructor;
import org.apache.shardingsphere.sql.parser.statement.core.value.identifier.IdentifierValue;

/**
 * Column assignment sql token.
 */
@RequiredArgsConstructor
public final class ColumnAssignmentSQLToken {
    
    private final IdentifierValue leftOwner;
    
    private final IdentifierValue leftColumnName;
    
    private final IdentifierValue rightOwner;
    
    private final IdentifierValue rightColumnName;
    
    @Override
    public String toString() {
        StringBuilder result = new StringBuilder();
        if (null != leftOwner) {
            result.append(leftOwner.getValueWithQuoteCharacters()).append(".");
        }
        result.append(leftColumnName.getValueWithQuoteCharacters()).append(" = ");
        if (null != rightOwner) {
            result.append(rightOwner.getValueWithQuoteCharacters()).append(".");
        }
        result.append(rightColumnName.getValueWithQuoteCharacters());
        return result.toString();
    }
}
