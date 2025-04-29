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

package com.sphereex.dbplusengine.infra.database.core.metadata.database.datatype;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.shardingsphere.infra.database.core.spi.DatabaseTypedSPI;
import org.apache.shardingsphere.infra.spi.annotation.SingletonSPI;

import java.util.Optional;

/**
 * Dialect data type extractor.
 */
@SingletonSPI
public interface DialectDataTypeExtractor extends DatabaseTypedSPI {
    
    /**
     * Extract dialect data type.
     *
     * @param dataTypeDefinition data type definition
     * @return data type
     */
    Optional<String> extract(String dataTypeDefinition);
    
    /**
     * Extract precision and scale.
     *
     * @param dataTypeDefinition data type definition
     * @return precision and scale
     */
    default Optional<Pair<Integer, Integer>> extractPrecisionAndScale(String dataTypeDefinition) {
        return Optional.empty();
    }
}
