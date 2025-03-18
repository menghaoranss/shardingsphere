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

package org.apache.shardingsphere.encrypt.rewrite.token.pojo;

import com.sphereex.dbplusengine.SphereEx;
import lombok.RequiredArgsConstructor;
import org.apache.shardingsphere.infra.database.core.metadata.database.enums.QuoteCharacter;
import org.apache.shardingsphere.sql.parser.statement.core.value.identifier.IdentifierValue;

import java.util.Collection;
import java.util.LinkedList;
import java.util.stream.Collectors;

/**
 * Literal assignment token for encrypt.
 */
public final class EncryptLiteralAssignmentToken extends EncryptAssignmentToken {
    
    private final Collection<LiteralAssignment> assignments = new LinkedList<>();
    
    public EncryptLiteralAssignmentToken(final int startIndex, final int stopIndex, final QuoteCharacter quoteCharacter, @SphereEx final IdentifierValue owner) {
        // SPEX CHANGED: BEGIN
        super(startIndex, stopIndex, quoteCharacter, owner);
        // SPEX CHANGED: END
    }
    
    /**
     * Add assignment.
     *
     * @param columnName column name
     * @param value assignment value
     */
    public void addAssignment(final String columnName, final Object value) {
        // SPEX CHANGED: BEGIN
        assignments.add(new LiteralAssignment(columnName, value, getQuoteCharacter(), getOwner()));
        // SPEX CHANGED: END
    }
    
    @Override
    public String toString() {
        return assignments.stream().map(LiteralAssignment::toString).collect(Collectors.joining(", "));
    }
    
    @RequiredArgsConstructor
    private static final class LiteralAssignment {
        
        private final String columnName;
        
        private final Object value;
        
        private final QuoteCharacter quoteCharacter;
        
        @SphereEx
        private final String owner;
        
        @Override
        public String toString() {
            // SPEX CHANGED: BEGIN
            return (owner.isEmpty() ? "" : owner + ".") + quoteCharacter.wrap(columnName) + " = " + toString(value);
            // SPEX CHANGED: END
        }
        
        private String toString(final Object value) {
            if (null == value) {
                return "NULL";
            }
            return value instanceof String ? "'" + value + "'" : value.toString();
        }
    }
}
