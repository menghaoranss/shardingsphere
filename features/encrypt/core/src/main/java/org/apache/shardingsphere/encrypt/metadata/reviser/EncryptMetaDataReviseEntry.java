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

package org.apache.shardingsphere.encrypt.metadata.reviser;

import com.sphereex.dbplusengine.SphereEx;
import com.sphereex.dbplusengine.encrypt.config.rule.mode.EncryptModeType;
import com.sphereex.dbplusengine.encrypt.metadata.reviser.column.EncryptColumnDataTypeReviser;
import com.sphereex.dbplusengine.encrypt.metadata.reviser.table.EncryptTableNameReviser;
import org.apache.shardingsphere.encrypt.constant.EncryptOrder;
import org.apache.shardingsphere.encrypt.metadata.reviser.column.EncryptColumnExistedReviser;
import org.apache.shardingsphere.encrypt.metadata.reviser.column.EncryptColumnNameReviser;
import org.apache.shardingsphere.encrypt.metadata.reviser.index.EncryptIndexReviser;
import org.apache.shardingsphere.encrypt.rule.EncryptRule;
import org.apache.shardingsphere.infra.metadata.database.schema.reviser.MetaDataReviseEntry;

import java.util.Optional;

/**
 * Encrypt meta data revise entry.
 */
public final class EncryptMetaDataReviseEntry implements MetaDataReviseEntry<EncryptRule> {
    
    @SphereEx
    @Override
    public Optional<EncryptTableNameReviser> getTableNameReviser() {
        return Optional.of(new EncryptTableNameReviser());
    }
    
    @Override
    public Optional<EncryptColumnExistedReviser> getColumnExistedReviser(final EncryptRule rule, final String tableName) {
        // SPEX ADDED: BEGIN
        if (EncryptModeType.BACKEND == rule.getEncryptMode().getType()) {
            return rule.findEncryptTableByActualTable(tableName).map(EncryptColumnExistedReviser::new);
        }
        // SPEX ADDED: END
        return rule.findEncryptTable(tableName).map(EncryptColumnExistedReviser::new);
    }
    
    @Override
    public Optional<EncryptColumnNameReviser> getColumnNameReviser(final EncryptRule rule, final String tableName) {
        // SPEX ADDED: BEGIN
        if (EncryptModeType.BACKEND == rule.getEncryptMode().getType()) {
            return rule.findEncryptTableByActualTable(tableName).map(EncryptColumnNameReviser::new);
        }
        // SPEX ADDED: END
        return rule.findEncryptTable(tableName).map(EncryptColumnNameReviser::new);
    }
    
    @SphereEx
    @Override
    public Optional<? extends EncryptColumnDataTypeReviser> getColumnDataTypeReviser(final EncryptRule rule, final String tableName) {
        if (EncryptModeType.BACKEND == rule.getEncryptMode().getType()) {
            return rule.findEncryptTableByActualTable(tableName).map(EncryptColumnDataTypeReviser::new);
        }
        return rule.findEncryptTable(tableName).map(EncryptColumnDataTypeReviser::new);
    }
    
    @Override
    public Optional<EncryptIndexReviser> getIndexReviser(final EncryptRule rule, final String tableName) {
        // SPEX ADDED: BEGIN
        if (EncryptModeType.BACKEND == rule.getEncryptMode().getType()) {
            return rule.findEncryptTableByActualTable(tableName).map(EncryptIndexReviser::new);
        }
        // SPEX ADDED: END
        return rule.findEncryptTable(tableName).map(EncryptIndexReviser::new);
    }
    
    @Override
    public int getOrder() {
        return EncryptOrder.ORDER;
    }
    
    @Override
    public Class<EncryptRule> getTypeClass() {
        return EncryptRule.class;
    }
}
