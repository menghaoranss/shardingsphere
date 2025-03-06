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

package com.sphereex.dbplusengine.infra.exception.kernel.syntax.hint;

import org.apache.shardingsphere.infra.exception.core.external.sql.sqlstate.XOpenSQLState;
import org.apache.shardingsphere.infra.exception.kernel.syntax.hint.HintSQLException;

/**
 * Data source hint not allowed exception.
 */
public final class DataSourceHintNotAllowedException extends HintSQLException {
    
    private static final long serialVersionUID = 7125419750640009196L;
    
    public DataSourceHintNotAllowedException(final String dataSources) {
        super(XOpenSQLState.SYNTAX_ERROR, 0, "Only allow one dataSource, but exist multiple dataSources: %s.", dataSources);
    }
}
