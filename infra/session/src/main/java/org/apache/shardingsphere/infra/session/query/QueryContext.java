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

package org.apache.shardingsphere.infra.session.query;

import com.sphereex.dbplusengine.SphereEx;
import lombok.Getter;
import lombok.Setter;
import org.apache.shardingsphere.infra.binder.context.statement.SQLStatementContext;
import org.apache.shardingsphere.infra.binder.context.type.TableAvailable;
import org.apache.shardingsphere.infra.exception.core.ShardingSpherePreconditions;
import org.apache.shardingsphere.infra.exception.dialect.exception.syntax.database.NoDatabaseSelectedException;
import org.apache.shardingsphere.infra.exception.dialect.exception.syntax.database.UnknownDatabaseException;
import org.apache.shardingsphere.infra.hint.HintValueContext;
import org.apache.shardingsphere.infra.metadata.ShardingSphereMetaData;
import org.apache.shardingsphere.infra.metadata.database.ShardingSphereDatabase;
import org.apache.shardingsphere.infra.session.connection.ConnectionContext;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

/**
 * Query context.
 */
@Getter
public final class QueryContext {
    
    private final SQLStatementContext sqlStatementContext;
    
    private final String sql;
    
    private final List<Object> parameters;
    
    private final HintValueContext hintValueContext;
    
    private final ConnectionContext connectionContext;
    
    private final ShardingSphereMetaData metaData;
    
    private final Collection<String> usedDatabaseNames;
    
    private final boolean useCache;
    
    @SphereEx
    @Setter
    private String defaultDataSourceName;
    
    public QueryContext(final SQLStatementContext sqlStatementContext, final String sql, final List<Object> params, final HintValueContext hintValueContext, final ConnectionContext connectionContext,
                        final ShardingSphereMetaData metaData) {
        this(sqlStatementContext, sql, params, hintValueContext, connectionContext, metaData, false);
    }
    
    public QueryContext(final SQLStatementContext sqlStatementContext, final String sql, final List<Object> params, final HintValueContext hintValueContext, final ConnectionContext connectionContext,
                        final ShardingSphereMetaData metaData, final boolean useCache) {
        this.sqlStatementContext = sqlStatementContext;
        this.sql = sql;
        parameters = params;
        this.hintValueContext = hintValueContext;
        this.connectionContext = connectionContext;
        this.metaData = metaData;
        usedDatabaseNames = getUsedDatabaseNames(sqlStatementContext, connectionContext);
        this.useCache = useCache;
    }
    
    private Collection<String> getUsedDatabaseNames(final SQLStatementContext sqlStatementContext, final ConnectionContext connectionContext) {
        if (sqlStatementContext instanceof TableAvailable) {
            Collection<String> result = ((TableAvailable) sqlStatementContext).getTablesContext().getDatabaseNames();
            return result.isEmpty() ? getCurrentDatabaseNames(connectionContext) : result;
        }
        return getCurrentDatabaseNames(connectionContext);
    }
    
    private Collection<String> getCurrentDatabaseNames(final ConnectionContext connectionContext) {
        return connectionContext.getCurrentDatabaseName().isPresent() ? Collections.singleton(connectionContext.getCurrentDatabaseName().get()) : Collections.emptyList();
    }
    
    /**
     * Get used database.
     *
     * @return used database
     */
    public ShardingSphereDatabase getUsedDatabase() {
        // SPEX DELETE: BEGIN
        // ShardingSpherePreconditions.checkState(usedDatabaseNames.size() <= 1,
        // () -> new UnsupportedSQLOperationException(String.format("Can not support multiple logic databases [%s]", Joiner.on(", ").join(usedDatabaseNames))));
        // SPEX DELETE: END
        String databaseName = usedDatabaseNames.iterator().next();
        ShardingSpherePreconditions.checkState(metaData.containsDatabase(databaseName), () -> new UnknownDatabaseException(databaseName));
        return metaData.getDatabase(databaseName);
    }
    
    /**
     * Get used databases.
     *
     * @return used databases
     */
    @SphereEx
    public Collection<ShardingSphereDatabase> getUsedDatabases() {
        Collection<ShardingSphereDatabase> result = new LinkedList<>();
        Collection<String> databaseNames = sqlStatementContext instanceof TableAvailable ? ((TableAvailable) sqlStatementContext).getTablesContext().getDatabaseNames() : Collections.emptyList();
        databaseNames = databaseNames.isEmpty() ? connectionContext.getCurrentDatabaseName().map(Collections::singleton).orElseGet(Collections::emptySet) : databaseNames;
        databaseNames.forEach(each -> result.add(getDatabase(each)));
        return result;
    }
    
    @SphereEx
    private ShardingSphereDatabase getDatabase(final String databaseName) {
        ShardingSpherePreconditions.checkNotNull(databaseName, NoDatabaseSelectedException::new);
        ShardingSpherePreconditions.checkState(metaData.containsDatabase(databaseName), () -> new UnknownDatabaseException(databaseName));
        return metaData.getDatabase(databaseName);
    }
}
