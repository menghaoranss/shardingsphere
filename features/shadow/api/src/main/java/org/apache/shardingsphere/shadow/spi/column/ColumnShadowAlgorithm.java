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

package org.apache.shardingsphere.shadow.spi.column;

import org.apache.shardingsphere.shadow.spi.ShadowOperationType;
import org.apache.shardingsphere.shadow.spi.ShadowAlgorithm;

/**
 * Column shadow algorithm.
 * 
 * @param <T> type of column shadow value
 */
public interface ColumnShadowAlgorithm<T extends Comparable<?>> extends ShadowAlgorithm {
    
    /**
     * Is need shadow.
     *
     * @param columnShadowValue column shadow value
     * @return is need shadow or not
     */
    boolean isShadow(PreciseColumnShadowValue<T> columnShadowValue);
    
    /**
     * Get shadow column.
     *
     * @return shadow column
     */
    String getShadowColumn();
    
    /**
     * Get shadow operation type.
     *
     * @return shadow operation type
     */
    ShadowOperationType getShadowOperationType();
}
