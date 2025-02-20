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

package com.sphereex.dbplusengine.infra.metadata.database.schema.reviser.column;

import com.sphereex.dbplusengine.SphereEx;
import org.apache.shardingsphere.infra.database.core.metadata.data.model.ColumnMetaData;
import org.apache.shardingsphere.infra.database.core.type.DatabaseType;

import javax.sql.DataSource;
import java.util.Collection;
import java.util.Optional;

/**
 * Column data type reviser.
 */
public interface ColumnDataTypeReviser {
    
    /**
     * Revise column data type.
     *
     * @param originalName original column name
     * @param databaseType database type
     * @param dataSource data source
     * @return revised data type
     */
    Optional<Integer> revise(String originalName, DatabaseType databaseType, DataSource dataSource);
    
    /**
     * Revise column data type content.
     *
     * @param originalName original column name
     * @param originalMetaDataList original column meta data list
     * @return revised column data type content
     */
    @SphereEx
    Optional<String> revise(String originalName, Collection<ColumnMetaData> originalMetaDataList);
    
    /**
     * Check column data type.
     *
     * @param columnMetaData column meta data
     * @param databaseType database type
     * @param dataSource data source
     */
    @SphereEx
    void check(ColumnMetaData columnMetaData, DatabaseType databaseType, DataSource dataSource);
}
