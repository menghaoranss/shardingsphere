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

import com.sphereex.dbplusengine.infra.rewrite.token.pojo.generic.ComposableSQLToken;
import com.sphereex.dbplusengine.infra.rewrite.token.pojo.generic.ComposableSQLTokenAvailable;
import lombok.Getter;
import org.apache.shardingsphere.infra.rewrite.sql.token.common.pojo.Attachable;
import org.apache.shardingsphere.infra.rewrite.sql.token.common.pojo.SQLToken;

/**
 * Encrypt scalar subquery attachable token.
 */
@Getter
public final class EncryptScalarSubqueryAttachableToken extends SQLToken implements Attachable, ComposableSQLTokenAvailable {
    
    private final ComposableSQLToken sqlToken;
    
    private final int stopIndex;
    
    public EncryptScalarSubqueryAttachableToken(final int startIndex, final int stopIndex, final ComposableSQLToken sqlToken) {
        super(startIndex);
        this.stopIndex = stopIndex;
        this.sqlToken = sqlToken;
    }
    
    @Override
    public ComposableSQLToken getComposableSQLToken() {
        return sqlToken;
    }
}
