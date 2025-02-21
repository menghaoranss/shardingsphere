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

package com.sphereex.dbplusengine.encrypt.exception.metadata;

import com.sphereex.dbplusengine.encrypt.rule.mode.EncryptMode;
import org.apache.shardingsphere.encrypt.exception.EncryptSQLException;
import org.apache.shardingsphere.infra.exception.core.external.sql.sqlstate.XOpenSQLState;

/**
 * Missing matched encrypt mode SQL exception.
 */
public final class MissingMatchedEncryptModeSQLException extends EncryptSQLException {
    
    private static final long serialVersionUID = -2079374357109432304L;
    
    public MissingMatchedEncryptModeSQLException(final EncryptMode encryptMode, final String featureType) {
        super(XOpenSQLState.GENERAL_ERROR, 5, "Missing configuration of `%s` mode `%s` props.", encryptMode.getType(), featureType);
    }
}
