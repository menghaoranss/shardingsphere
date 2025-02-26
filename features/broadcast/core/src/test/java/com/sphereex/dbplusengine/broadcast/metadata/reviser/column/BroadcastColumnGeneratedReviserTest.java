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

package com.sphereex.dbplusengine.broadcast.metadata.reviser.column;

import com.sphereex.dbplusengine.broadcast.config.keygen.BroadcastKeyGenerateStrategyConfiguration;
import org.apache.shardingsphere.infra.database.core.metadata.data.model.ColumnMetaData;
import org.junit.jupiter.api.Test;

import java.sql.Types;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BroadcastColumnGeneratedReviserTest {
    
    @Test
    void assertRevise() {
        BroadcastColumnGeneratedReviser reviser = new BroadcastColumnGeneratedReviser(new BroadcastKeyGenerateStrategyConfiguration("t_config", "id", "snowflake"));
        assertTrue(reviser.revise(new ColumnMetaData("id", Types.INTEGER, false, false, false, false, false, false, "")));
        assertFalse(reviser.revise(new ColumnMetaData("name", Types.VARCHAR, false, false, false, false, false, false, "")));
    }
}
