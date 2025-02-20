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

import lombok.Getter;
import org.apache.shardingsphere.infra.rewrite.sql.token.common.pojo.SQLToken;
import org.apache.shardingsphere.infra.rewrite.sql.token.common.pojo.Substitutable;

import java.util.Collection;
import java.util.Map;

/**
 * Between value token for encrypt.
 */
public final class EncryptBetweenValueToken extends SQLToken implements Substitutable {
    
    @Getter
    private final int stopIndex;
    
    private final Map<Integer, Object> indexValues;
    
    private final Collection<Integer> paramMarkerIndexes;
    
    public EncryptBetweenValueToken(final int startIndex, final int stopIndex, final Map<Integer, Object> indexValues, final Collection<Integer> paramMarkerIndexes) {
        super(startIndex);
        this.stopIndex = stopIndex;
        this.indexValues = indexValues;
        this.paramMarkerIndexes = paramMarkerIndexes;
    }
    
    @Override
    public String toString() {
        StringBuilder result = new StringBuilder();
        for (int i = 0; i < indexValues.size() + paramMarkerIndexes.size(); i++) {
            if (paramMarkerIndexes.contains(i)) {
                result.append('?');
            } else {
                if (indexValues.get(i) instanceof String) {
                    result.append('\'').append(indexValues.get(i)).append('\'');
                } else {
                    result.append(indexValues.get(i));
                }
            }
            result.append(" AND ");
        }
        result.delete(result.length() - 4, result.length());
        return result.toString();
    }
}
