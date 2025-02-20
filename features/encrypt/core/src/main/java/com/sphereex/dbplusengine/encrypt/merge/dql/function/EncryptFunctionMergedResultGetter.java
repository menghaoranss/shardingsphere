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

package com.sphereex.dbplusengine.encrypt.merge.dql.function;

import org.apache.shardingsphere.encrypt.rule.EncryptRule;
import org.apache.shardingsphere.infra.database.core.type.DatabaseType;
import org.apache.shardingsphere.infra.merge.result.MergedResult;
import org.apache.shardingsphere.infra.spi.annotation.SingletonSPI;
import org.apache.shardingsphere.infra.spi.type.typed.TypedSPI;
import org.apache.shardingsphere.sql.parser.statement.core.segment.dml.expr.ExpressionSegment;

import java.sql.SQLException;

/**
 * Encrypt function merged result getter.
 */
@SingletonSPI
public interface EncryptFunctionMergedResultGetter extends TypedSPI {
    
    /**
     * Judge is need decrypt or not.
     *
     * @param expressionSegment expression segment
     * @return is need decrypt or not
     */
    boolean isNeedDecrypt(ExpressionSegment expressionSegment);
    
    /**
     * Get value.
     *
     * @param mergedResult merged result
     * @param encryptRule encrypt rule
     * @param databaseType database type
     * @param expressionSegment expression segment
     * @param columnIndex column index
     * @param type class type of data value
     * @return data value
     * @throws SQLException SQL exception
     */
    Object getValue(MergedResult mergedResult, EncryptRule encryptRule, DatabaseType databaseType, ExpressionSegment expressionSegment, int columnIndex, Class<?> type) throws SQLException;
}
