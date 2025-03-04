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

import com.google.common.base.Joiner;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.apache.shardingsphere.infra.rewrite.sql.token.common.pojo.SQLToken;
import org.apache.shardingsphere.infra.rewrite.sql.token.common.pojo.Substitutable;
import org.apache.shardingsphere.sql.parser.statement.core.enums.ParameterMarkerType;

import java.util.Collection;
import java.util.LinkedList;

/**
 * Select value token for encrypt.
 */
@Getter
public final class EncryptSelectValueToken extends SQLToken implements Substitutable {
    
    private final int stopIndex;
    
    private final Collection<SelectValue> values = new LinkedList<>();
    
    public EncryptSelectValueToken(final int startIndex, final int stopIndex) {
        super(startIndex);
        this.stopIndex = stopIndex;
    }
    
    /**
     * Add value.
     *
     * @param value value
     */
    public void addValue(final Object value) {
        values.add(new SelectValue(value));
    }
    
    /**
     * Add value.
     *
     * @param value value
     * @param alias alias
     */
    public void addValue(final Object value, final String alias) {
        values.add(new SelectValue(value, alias));
    }
    
    @Override
    public String toString() {
        return Joiner.on(", ").join(values);
    }
    
    @RequiredArgsConstructor
    private static final class SelectValue {
        
        private final Object value;
        
        private final String alias;
        
        SelectValue(final Object value) {
            this.value = value;
            this.alias = null;
        }
        
        @Override
        public String toString() {
            return toString(value) + (null == alias ? "" : " AS " + alias);
        }
        
        private String toString(final Object value) {
            if (null == value) {
                return "NULL";
            }
            if (value instanceof String && ParameterMarkerType.QUESTION.getMarker().equals(String.valueOf(value))) {
                return ParameterMarkerType.QUESTION.getMarker();
            }
            if (value instanceof String && ParameterMarkerType.DOLLAR.getMarker().equals(String.valueOf(value))) {
                return ParameterMarkerType.DOLLAR.getMarker();
            }
            return value instanceof String ? "'" + value + "'" : value.toString();
        }
    }
}
