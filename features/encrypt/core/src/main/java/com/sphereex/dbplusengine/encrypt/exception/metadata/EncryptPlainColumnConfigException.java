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

import org.apache.shardingsphere.encrypt.exception.EncryptSQLException;
import org.apache.shardingsphere.infra.exception.core.external.sql.sqlstate.XOpenSQLState;

/**
 * Encrypt plain column config exception.
 */
public final class EncryptPlainColumnConfigException extends EncryptSQLException {
    
    private static final long serialVersionUID = 1051383260057551699L;
    
    public EncryptPlainColumnConfigException(final String plainColumn, final String logicColumn) {
        super(XOpenSQLState.CHECK_OPTION_VIOLATION, 52, "Plain column '%s' must config same name with logical column '%s'.", plainColumn, logicColumn);
    }
}
