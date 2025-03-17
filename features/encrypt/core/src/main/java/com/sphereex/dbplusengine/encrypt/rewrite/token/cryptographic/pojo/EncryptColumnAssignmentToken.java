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

package com.sphereex.dbplusengine.encrypt.rewrite.token.cryptographic.pojo;

import com.sphereex.dbplusengine.infra.rewrite.token.pojo.generic.ColumnAssignmentSQLToken;
import org.apache.shardingsphere.encrypt.rewrite.token.pojo.EncryptAssignmentToken;
import org.apache.shardingsphere.infra.database.core.metadata.database.enums.QuoteCharacter;
import org.apache.shardingsphere.infra.exception.core.ShardingSpherePreconditions;
import org.apache.shardingsphere.sql.parser.statement.core.value.identifier.IdentifierValue;

import java.util.Collection;
import java.util.LinkedList;
import java.util.stream.Collectors;

/**
 * Column assignment token for encrypt.
 */
public final class EncryptColumnAssignmentToken extends EncryptAssignmentToken {
    
    private final Collection<ColumnAssignmentSQLToken> assignments = new LinkedList<>();
    
    public EncryptColumnAssignmentToken(final int startIndex, final int stopIndex, final QuoteCharacter quoteCharacter) {
        // SPEX CHANGED: BEGIN
        super(startIndex, stopIndex, quoteCharacter, null);
        // SPEX CHANGED: END
    }
    
    /**
     * Add assignment.
     *
     * @param leftOwner left owner
     * @param leftColumnName left column name
     * @param rightOwner right owner
     * @param rightColumnName right column name
     */
    public void addAssignment(final IdentifierValue leftOwner, final IdentifierValue leftColumnName, final IdentifierValue rightOwner, final IdentifierValue rightColumnName) {
        assignments.add(new ColumnAssignmentSQLToken(leftOwner, leftColumnName, rightOwner, rightColumnName));
    }
    
    @Override
    public void addAssignment(final String columnName, final Object value) {
        ShardingSpherePreconditions.checkState(value instanceof ColumnAssignmentSQLToken, () -> new IllegalArgumentException("value must be ColumnAssignment type"));
        assignments.add((ColumnAssignmentSQLToken) value);
    }
    
    @Override
    public String toString() {
        return assignments.stream().map(ColumnAssignmentSQLToken::toString).collect(Collectors.joining(", "));
    }
}
